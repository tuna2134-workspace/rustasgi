import http.client
import json

from _util import StdServer, free_port


def test_fastapi_routes():
    port = free_port()
    srv = StdServer("examples.fastapi_app:app", port)
    try:
        status, _, data = srv.request("GET", "/")
        assert status == 200, data
        assert json.loads(data.decode()) == {"message": "hello from rustwasgi"}

        status, _, data = srv.request("GET", "/async")
        assert status == 200, data
        assert json.loads(data.decode()) == {"async": True}

        body = json.dumps({"hello": "world"}).encode()
        status, _, data = srv.request(
            "POST", "/echo", body=body,
            headers={"Content-Type": "application/json"},
        )
        assert status == 200, data
        assert json.loads(data.decode()) == {"hello": "world"}

        status, headers, data = srv.request("GET", "/text")
        assert status == 200, data
        assert data == b"hello"
        ctype = dict((k.lower(), v) for k, v in headers).get("content-type", "")
        assert "text/plain" in ctype, headers

        status, _, data = srv.request("GET", "/healthz")
        assert status == 200, data
        assert json.loads(data.decode()) == {"status": "ok"}
    finally:
        srv.stop_and_assert_clean()


def test_fastapi_streaming_response_is_incremental():
    """First chunk must arrive well before the generator finishes."""
    import time

    port = free_port()
    srv = StdServer("examples.fastapi_app:app", port)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            start = time.monotonic()
            conn.request("GET", "/stream")
            resp = conn.getresponse()
            assert resp.status == 200, resp.status
            first = resp.read(2)  # "0\n"
            first_at = time.monotonic() - start
            rest = resp.read()
            total_at = time.monotonic() - start
            body = (first + rest).decode()
            assert body == "".join(f"{i}\n" for i in range(100)), body[:50]
            # 100 chunks x 1ms + overhead; buffered would deliver all at once
            # at ~total; streaming delivers the first chunk early.
            assert first_at < total_at, (first_at, total_at)
            assert first == b"0\n", first
        finally:
            conn.close()
    finally:
        srv.stop_and_assert_clean()


def test_application_factory_call_syntax():
    port = free_port()
    srv = StdServer("examples.factory_app:create_app()", port)
    try:
        status, _, data = srv.request("GET", "/")
        assert status == 200, data
        assert json.loads(data.decode()) == {"factory": True}
    finally:
        srv.stop_and_assert_clean()


def test_fastapi_head_suppresses_body():
    # Starlette 405s HEAD on this GET-only route; the 405 comes *from the
    # app*, proving scope["method"] == "HEAD" propagation, and the wire
    # body must still be empty.
    port = free_port()
    srv = StdServer("examples.fastapi_app:app", port)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        try:
            conn.request("HEAD", "/text")
            resp = conn.getresponse()
            body = resp.read()
            assert resp.status == 405, resp.status
            assert body == b"", body
        finally:
            conn.close()
    finally:
        srv.stop_and_assert_clean()
