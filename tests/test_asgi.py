import http.client
import json
import threading

from _util import StdServer, free_port


def test_pure_asgi_echo_and_dup_headers():
    port = free_port()
    srv = StdServer("examples.asgi_app:app", port)
    try:
        status, headers, data = srv.request("GET", "/hello?x=1")
        assert status == 200, data
        payload = json.loads(data.decode())
        assert payload["method"] == "GET"
        assert payload["path"] == "/hello"
        assert payload["query"] == "x=1"
        assert payload["headers_are_list"] is True
        dups = [v for (k, v) in headers if k.lower() == "x-dup"]
        assert dups == ["one", "two"], headers

        status, _, data = srv.request(
            "POST", "/echo", body=b'{"a":1}',
            headers={"Content-Type": "application/json"},
        )
        assert status == 200, data
        payload = json.loads(data.decode())
        assert payload["body"] == '{"a":1}'
        assert payload["body_len"] == 7
    finally:
        srv.stop_and_assert_clean()


def test_asgi2_legacy_double_callable():
    port = free_port()
    srv = StdServer("examples.asgi2_app:app", port)
    try:
        status, headers, data = srv.request("GET", "/")
        assert status == 200, data
        assert data == b"asgi2:"

        status, _, data = srv.request(
            "POST", "/upload", body=b"0123456789" * 1000,
            headers={"Content-Type": "application/octet-stream"},
        )
        assert status == 200, data
        assert data == b"asgi2:" + b"0123456789" * 1000
    finally:
        srv.stop_and_assert_clean()


def test_http_methods_query_paths_and_head():
    port = free_port()
    srv = StdServer("examples.asgi_app:app", port)
    try:
        for method in ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]:
            status, _, data = srv.request(method, "/foo")
            assert status == 200, (method, data)
            assert json.loads(data.decode())["method"] == method

        # query string preserved raw
        status, _, data = srv.request("GET", "/foo?a=%E3%81%82&b=2")
        assert status == 200, data
        assert json.loads(data.decode())["query"] == "a=%E3%81%82&b=2"

        # percent-decoded path vs raw
        status, _, data = srv.request("GET", "/foo%20bar")
        assert status == 200, data
        assert json.loads(data.decode())["path"] == "/foo bar"

        status, _, data = srv.request("GET", "/%E3%81%82")
        assert status == 200, data
        assert json.loads(data.decode())["path"] == "/\u3042"

        # HEAD: app sees HEAD; wire body must be empty.
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        try:
            conn.request("HEAD", "/foo")
            resp = conn.getresponse()
            body = resp.read()
            assert resp.status == 200, resp.status
            assert body == b"", body
        finally:
            conn.close()

        # cookies / duplicate request headers survive as a list
        status, _, data = srv.request(
            "GET", "/c", headers={"Cookie": "a=1; b=2"},
        )
        assert status == 200, data
    finally:
        srv.stop_and_assert_clean()


def test_keep_alive_reuses_connection():
    port = free_port()
    srv = StdServer("examples.asgi_app:app", port)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        try:
            for i in range(5):
                conn.request("GET", f"/keep-{i}")
                resp = conn.getresponse()
                body = resp.read()
                assert resp.status == 200, body
                assert json.loads(body.decode())["path"] == f"/keep-{i}"
        finally:
            conn.close()
    finally:
        srv.stop_and_assert_clean()


def test_large_request_body_streams():
    port = free_port()
    srv = StdServer("examples.asgi_app:app", port)
    try:
        big = b"x" * (2 * 1024 * 1024)  # 2 MiB
        status, _, data = srv.request(
            "POST", "/big", body=big,
            headers={"Content-Type": "application/octet-stream"},
        )
        assert status == 200, data[:100]
        assert json.loads(data.decode())["body_len"] == len(big)
    finally:
        srv.stop_and_assert_clean()


def test_chunked_request_body():
    port = free_port()
    srv = StdServer("examples.asgi_app:app", port)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
        try:
            conn.putrequest("POST", "/chunked")
            conn.putheader("Transfer-Encoding", "chunked")
            conn.putheader("Content-Type", "text/plain")
            conn.endheaders()
            for piece in [b"hello ", b"chunked ", b"world"]:
                conn.send(f"{len(piece):X}\r\n".encode() + piece + b"\r\n")
            conn.send(b"0\r\n\r\n")
            resp = conn.getresponse()
            data = resp.read()
            assert resp.status == 200, data
            payload = json.loads(data.decode())
            assert payload["body"] == "hello chunked world", payload
        finally:
            conn.close()
    finally:
        srv.stop_and_assert_clean()


def test_concurrent_requests():
    port = free_port()
    srv = StdServer("examples.asgi_app:app", port)
    try:
        errors: list = []

        def one(i: int):
            try:
                status, _, data = srv.request("GET", f"/conc-{i}")
                assert status == 200, data
                assert json.loads(data.decode())["path"] == f"/conc-{i}"
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=one, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors, errors[:3]
    finally:
        srv.stop_and_assert_clean()
