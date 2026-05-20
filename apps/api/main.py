from fastapi import FastAPI

from shared.config import get_settings

app = FastAPI(title="Saas-Poly API", version="0.1.0")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
async def root() -> dict[str, str]:
    settings = get_settings()
    return {"service": "saas-poly-api", "log_level": settings.log_level}
