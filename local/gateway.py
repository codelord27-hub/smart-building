"""Local stand-in for the GKE Ingress: routes by path prefix exactly like k8s/apps/ingress.yaml."""
import os

import httpx
from fastapi import FastAPI, Request, Response

ROUTES = [
    ("/api/readings", os.environ.get("INGEST_URL", "http://127.0.0.1:8002")),
    ("/api/occupancy", os.environ.get("OCCUPANCY_URL", "http://127.0.0.1:8003")),
    ("/api/energy", os.environ.get("ENERGY_URL", "http://127.0.0.1:8004")),
    ("/api/control", os.environ.get("CONTROL_URL", "http://127.0.0.1:8005")),
    ("/api/hvac", os.environ.get("HVAC_URL", "http://127.0.0.1:8006")),
    ("/", os.environ.get("REGISTRY_URL", "http://127.0.0.1:8001")),
]
app = FastAPI()
client = httpx.AsyncClient(timeout=60)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy(path: str, request: Request):
    url_path = "/" + path
    target = next(t for prefix, t in ROUTES if url_path.startswith(prefix))
    try:
        r = await client.request(request.method, target + url_path, params=request.query_params,
                                 content=await request.body(),
                                 headers={k: v for k, v in request.headers.items() if k.lower() != "host"})
    except httpx.HTTPError as exc:
        return Response(f"upstream error: {exc}", status_code=502)
    return Response(r.content, status_code=r.status_code,
                    headers={k: v for k, v in r.headers.items() if k.lower() not in ("content-length", "transfer-encoding",
                                                                                     "content-encoding")})
