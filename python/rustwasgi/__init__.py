"""rustwasgi — Rust (hyper/Tokio) ASGI server.

Standalone dev server::

    import rustwasgi
    rustwasgi.run("app:app")

Production (Gunicorn custom worker)::

    gunicorn -k rustwasgi.gunicorn.RustWASGIWorker -w 4 app:app
"""

from rustwasgi._rustwasgi import __version__, run, run_worker

__all__ = ["run", "run_worker", "__version__"]
