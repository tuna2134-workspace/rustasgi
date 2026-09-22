"""Legacy ASGI 2 double-callable application (compatibility test)."""


def app(scope):
    assert scope["type"] == "http", scope

    async def instance(receive, send):
        body = b""
        while True:
            message = await receive()
            if message["type"] != "http.request":
                break
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send(
            {"type": "http.response.body", "body": b"asgi2:" + body, "more_body": False}
        )

    return instance
