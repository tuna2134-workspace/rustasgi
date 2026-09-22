import json
import os

from _util import StdServer, free_port


def test_lifespan_startup_state_and_shutdown(tmp_path):
    marker = tmp_path / "lifespan.log"
    env_before = os.environ.get("RUSTWASGI_TEST_LIFESPAN_FILE")
    os.environ["RUSTWASGI_TEST_LIFESPAN_FILE"] = str(marker)
    port = free_port()
    srv = StdServer("examples.fastapi_lifespan:app", port)
    try:
        # startup ran before accepting requests
        text = marker.read_text()
        assert "startup" in text, text

        status, _, data = srv.request("GET", "/")
        assert status == 200, data
    finally:
        out = srv.stop_and_assert_clean()
    # shutdown ran during graceful shutdown
    text = marker.read_text()
    assert "shutdown" in text, text + "\n--- server output ---\n" + out
    if env_before is None:
        os.environ.pop("RUSTWASGI_TEST_LIFESPAN_FILE", None)
    else:
        os.environ["RUSTWASGI_TEST_LIFESPAN_FILE"] = env_before


def test_lifespan_state_propagates_to_http_scope():
    # Raw ASGI app sending explicit state in lifespan.startup.complete.
    port = free_port()
    srv = StdServer("examples.lifespan_state_app:app", port)
    try:
        status, _, data = srv.request("GET", "/")
        assert status == 200, data
        assert json.loads(data.decode()) == {"answer": 42}, data
    finally:
        srv.stop_and_assert_clean()


def test_lifespan_unsupported_app_still_serves():
    # examples.asgi_app asserts scope["type"] == "http": lifespan raises
    # inside the app -> server continues in auto mode.
    port = free_port()
    srv = StdServer("examples.asgi_app:app", port)
    try:
        status, _, data = srv.request("GET", "/")
        assert status == 200, data
    finally:
        srv.stop_and_assert_clean()
