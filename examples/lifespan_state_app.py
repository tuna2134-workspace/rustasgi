"""Raw ASGI app with explicit lifespan state (spec-level state propagation)."""

import json


async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send(
                    {
                        "type": "lifespan.startup.complete",
                        "state": {"answer": 42},
                    }
                )
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
        return

    assert scope["type"] == "http"
    body = json.dumps({"answer": scope.get("state", {}).get("answer")}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": body})
