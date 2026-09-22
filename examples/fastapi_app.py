import asyncio
import os

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse

app = FastAPI()


@app.get("/pid")
async def pid():
    return {"pid": os.getpid()}


@app.get("/")
async def root():
    return {"message": "hello from rustwasgi"}


@app.get("/async")
async def async_endpoint():
    await asyncio.sleep(0.01)
    return {"async": True}


@app.post("/echo")
async def echo(data: dict):
    return data


@app.get("/text")
async def text():
    return PlainTextResponse("hello")


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


async def _numbers():
    for i in range(100):
        yield f"{i}\n".encode()
        await asyncio.sleep(0.001)


@app.get("/stream")
async def stream():
    return StreamingResponse(_numbers(), media_type="text/plain")
