"""FastAPI app with lifespan (startup/shutdown + state propagation)."""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request


def _mark(event: str) -> None:
    path = os.environ.get("RUSTWASGI_TEST_LIFESPAN_FILE")
    if path:
        with open(path, "a") as f:
            f.write(f"{event} pid={os.getpid()}\n")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _mark("startup")
    app.state.startup_done = True
    app.state.value = 42
    yield {"answer": 42}
    _mark("shutdown")
    app.state.shutdown_done = True


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"lifespan": True}


@app.get("/state")
async def state(request: Request):
    # Lifespan-provided state is visible as scope["state"].
    return {"answer": request.scope.get("state", {}).get("answer")}
