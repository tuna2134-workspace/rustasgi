"""Minimal benchmark ASGI application (spec-mandated shape)."""

import os

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"ok": True}


@app.get("/pid")
async def pid():
    return {"pid": os.getpid()}
