"""Application factory (``module:create_app()`` call syntax)."""


def create_app():
    from fastapi import FastAPI

    app = FastAPI()

    @app.get("/")
    async def root():
        return {"factory": True}

    return app
