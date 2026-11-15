"""FastAPI app factory with Prometheus RED metrics, health endpoints and request logging."""
import logging
import time

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from .config import SERVICE_NAME

HTTP_REQUESTS = Counter(
    "http_requests_total",
    "HTTP requests handled",
    ["service", "method", "route", "status"],
)
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["service", "method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

_SKIP_LOG = {"/healthz", "/readyz", "/metrics"}
log = logging.getLogger("http")


def create_app(title: str, docs_prefix: str, lifespan=None) -> FastAPI:
    app = FastAPI(
        title=title,
        docs_url=f"{docs_prefix}/docs",
        openapi_url=f"{docs_prefix}/openapi.json",
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.ready = True

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            route_path = getattr(route, "path", "unmatched")
            elapsed = time.perf_counter() - start
            if route_path not in _SKIP_LOG:
                HTTP_REQUESTS.labels(SERVICE_NAME, request.method, route_path, str(status)).inc()
                HTTP_LATENCY.labels(SERVICE_NAME, request.method, route_path).observe(elapsed)
                if status >= 500:
                    log.error("request failed", extra={"method": request.method, "route": route_path,
                                                       "status": status, "latency_ms": round(elapsed * 1000, 2)})

    @app.get("/healthz", include_in_schema=False)
    def healthz():
        return {"status": "ok", "service": SERVICE_NAME}

    @app.get("/readyz", include_in_schema=False)
    def readyz(response: Response):
        if not app.state.ready:
            response.status_code = 503
            return {"status": "starting"}
        return {"status": "ready"}

    @app.get("/metrics", include_in_schema=False)
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
