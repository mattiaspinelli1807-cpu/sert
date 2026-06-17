"""
Server di signaling + file statici per l'app vocale "Parla con gli amici".

Un solo processo, una sola porta:
  - le richieste HTTP normali ricevono index.html (e gli altri file);
  - le richieste su /ws diventano connessioni WebSocket usate SOLO per il
    "signaling" WebRTC (scambio di offerte/risposte SDP e candidati ICE) e
    per la chat testuale.

L'audio e il video NON passano da qui: viaggiano direttamente tra i browser
(peer-to-peer). Questo server mette solo in contatto i partecipanti.
"""

import asyncio
import json
import os
import secrets
from http import HTTPStatus
from pathlib import Path

from websockets.asyncio.server import serve
from websockets.datastructures import Headers
from websockets.http11 import Response

HERE = Path(__file__).resolve().parent
HOST = "0.0.0.0"          # ascolta su tutte le interfacce (necessario per LAN/tunnel)
# In locale usa 8000; in cloud (Render/Railway/Fly...) usa la porta data via $PORT.
PORT = int(os.environ.get("PORT", "8000"))

# Tipi di file che sappiamo servire (basta per questa app).
CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".ico": "image/x-icon",
}

# Stato in memoria: stanza -> { peer_id -> websocket }
ROOMS: dict[str, dict[str, object]] = {}
# websocket -> info del partecipante (per pulire alla disconnessione)
CLIENTS: dict[object, dict] = {}


def serve_static(request) -> Response | None:
    """Restituisce un file statico, oppure None per lasciar proseguire il WebSocket."""
    path = request.path.split("?")[0]
    if path == "/ws":
        return None  # Ã¨ una richiesta di signaling: la gestisce handler()

    if path == "/":
        path = "/index.html"

    target = (HERE / path.lstrip("/")).resolve()
    # Sicurezza: non uscire dalla cartella del progetto.
    if HERE not in target.parents and target != HERE:
        return Response(HTTPStatus.FORBIDDEN, "Forbidden", Headers(), b"")

    if not target.is_file():
        return Response(HTTPStatus.NOT_FOUND, "Not Found", Headers(), b"Not Found")

    body = target.read_bytes()
    ctype = CONTENT_TYPES.get(target.suffix, "application/octet-stream")
    headers = Headers({"Content-Type": ctype, "Content-Length": str(len(body))})
    return Response(HTTPStatus.OK, "OK", headers, body)


async def send(ws, obj) -> None:
    """Invia un messaggio JSON a un singolo websocket, ignorando se Ã¨ giÃ  chiuso."""
    try:
        await ws.send(json.dumps(obj))
    except Exception:
        pass


async def broadcast(room: str, obj, *, skip=None) -> None:
    """Invia un messaggio a tutti nella stanza, tranne eventualmente 'skip'."""
    for peer_id, peer_ws in list(ROOMS.get(room, {}).items()):
        if peer_ws is skip:
            continue
        await send(peer_ws, obj)


async def leave(ws) -> None:
    """Rimuove un partecipante e avvisa gli altri."""
    info = CLIENTS.pop(ws, None)
    if not info:
        return
    room, peer_id = info["room"], info["id"]
    ROOMS.get(room, {}).pop(peer_id, None)
    if not ROOMS.get(room):
        ROOMS.pop(room, None)  # stanza vuota: la eliminiamo
    await broadcast(room, {"type": "peer-left", "id": peer_id})


async def handler(ws) -> None:
    """Gestisce una connessione WebSocket di un partecipante."""
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            kind = msg.get("type")

            if kind == "join":
                room = str(msg.get("room", "")).strip() or "lobby"
                name = str(msg.get("name", "")).strip()[:40] or "Ospite"
                peer_id = secrets.token_hex(4)

                CLIENTS[ws] = {"id": peer_id, "room": room, "name": name}
                ROOMS.setdefault(room, {})[peer_id] = ws

                # Elenco dei partecipanti giÃ  presenti (escluso il nuovo arrivato).
                peers = [
                    {"id": pid, "name": CLIENTS[pws]["name"]}
                    for pid, pws in ROOMS[room].items()
                    if pws is not ws
                ]
                await send(ws, {"type": "welcome", "id": peer_id, "peers": peers})
                # Avvisa gli altri: saranno LORO a inviare l'offerta al nuovo arrivato.
                await broadcast(
                    room,
                    {"type": "peer-joined", "id": peer_id, "name": name},
                    skip=ws,
                )

            elif kind == "signal":
                # Inoltro "diretto" del messaggio di signaling al destinatario.
                info = CLIENTS.get(ws)
                if not info:
                    continue
                target = ROOMS.get(info["room"], {}).get(msg.get("to"))
                if target:
                    await send(target, {
                        "type": "signal",
                        "from": info["id"],
                        "data": msg.get("data"),
                    })

            elif kind == "chat":
                info = CLIENTS.get(ws)
                if not info:
                    continue
                text = str(msg.get("text", ""))[:2000]
                if text.strip():
                    await broadcast(info["room"], {
                        "type": "chat",
                        "from": info["id"],
                        "name": info["name"],
                        "text": text,
                    })
    finally:
        await leave(ws)


async def main() -> None:
    async with serve(handler, HOST, PORT, process_request=lambda c, r: serve_static(r)):
        print(f"Server avviato. Apri:  http://localhost:{PORT}")
        print("Per fermarlo: Ctrl+C")
        await asyncio.Future()  # gira per sempre


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer fermato.")
