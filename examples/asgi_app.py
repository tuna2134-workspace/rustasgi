"""Minimal pure-ASGI app (no FastAPI) to isolate ASGI compatibility.

Drains the request body with repeated receive() calls (streaming-safe),
then echoes scope details so tests prove scope/receive/send are really used.
"""


async def app(scope, receive, send):
    assert scope["type"] == "http", scope

    body = b""
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return
        if message["type"] != "http.request":
            continue
        body += message.get("body", b"")
        if not message.get("more_body"):
            break

    import json

    payload = json.dumps(
        {
            "method": scope["method"],
            "path": scope["path"],
            "query": scope["query_string"].decode("latin-1"),
            "headers_are_list": isinstance(scope["headers"], list),
            "body_len": len(body),
            "body": body.decode("utf-8", "replace"),
        }
    ).encode()

    headers = [
        (b"content-type", b"application/json"),
        (b"x-dup", b"one"),
        (b"x-dup", b"two"),  # duplicate response headers must survive
    ]
    await send({"type": "http.response.start", "status": 200, "headers": headers})
    await send({"type": "http.response.body", "body": payload, "more_body": False})
