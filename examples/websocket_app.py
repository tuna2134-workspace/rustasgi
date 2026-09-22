"""FastAPI WebSocket echo app (spec section: WebSocket FastAPI test)."""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            if data == "close":
                await websocket.close(code=1000)
                return
            await websocket.send_text(data)
    except WebSocketDisconnect:
        pass


@app.websocket("/ws-bytes")
async def websocket_bytes(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_bytes()
            await websocket.send_bytes(data)
    except WebSocketDisconnect:
        pass
