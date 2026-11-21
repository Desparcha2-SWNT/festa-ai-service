"""festa-ai-service: RF8 (recomendaciones conversacionales) y RF9 (descripciones con IA)."""
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from . import db, ratelimit
from .chat import router as chat_router
from .config import get_settings
from .descriptions import router as descriptions_router

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "info").upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("ai-service")

REQUESTS = Counter("http_requests_total", "Peticiones HTTP", ["method", "route", "status"])
LATENCY = Histogram("http_request_duration_seconds", "Latencia HTTP", ["route"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    if settings.effective_provider == "fake":
        log.warning("sin LLM_API_KEY: se usa el proveedor local 'fake' (sin costo, respuestas de ejemplo)")
    messaging = None
    if not app.state.skip_infra:
        await db.connect()
        await ratelimit.connect()
        from .messaging.supervisor import Messaging

        messaging = Messaging()
        await messaging.start()
    log.info("festa-ai-service arriba en el puerto %s con %s", settings.port, settings.effective_provider)
    yield
    if messaging:
        await messaging.stop()
    await ratelimit.close()
    await db.close()


def create_app(skip_infra: bool = False) -> FastAPI:
    app = FastAPI(title="festa-ai-service", version="1.0.0", lifespan=lifespan)
    app.state.skip_infra = skip_infra

    @app.middleware("http")
    async def metrics(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        route = getattr(request.scope.get("route"), "path", "unmatched")
        REQUESTS.labels(request.method, route, response.status_code).inc()
        LATENCY.labels(route).observe(time.perf_counter() - start)
        return response

    @app.exception_handler(Exception)
    async def unexpected(request: Request, exc: Exception):
        log.exception("error inesperado")
        return JSONResponse(status_code=500, content={"detail": "error interno"})

    @app.get("/health")
    async def health():
        return {"status": "ok", "llmProvider": get_settings().effective_provider}

    @app.get("/metrics")
    async def prometheus():
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    # El cliente llama a /api/ai/...; Kong quita el "/api".
    app.include_router(descriptions_router)
    app.include_router(chat_router)
    return app


app = create_app()
