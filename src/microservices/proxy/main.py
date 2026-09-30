import os
import random
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response


MONOLITH_URL = os.getenv(
    "MONOLITH_URL",
    "http://monolith:8080",
).rstrip("/")

MOVIES_SERVICE_URL = os.getenv(
    "MOVIES_SERVICE_URL",
    "http://movies-service:8081",
).rstrip("/")

EVENTS_SERVICE_URL = os.getenv(
    "EVENTS_SERVICE_URL",
    "http://events-service:8082",
).rstrip("/")

GRADUAL_MIGRATION = (
    os.getenv("GRADUAL_MIGRATION", "true").strip().lower() == "true"
)

try:
    MOVIES_MIGRATION_PERCENT = int(
        os.getenv("MOVIES_MIGRATION_PERCENT", "0")
    )
except ValueError:
    MOVIES_MIGRATION_PERCENT = 0

MOVIES_MIGRATION_PERCENT = max(
    0,
    min(100, MOVIES_MIGRATION_PERCENT),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(30.0),
        follow_redirects=False,
    )

    yield

    await app.state.http_client.aclose()


app = FastAPI(
    title="CinemaAbyss Proxy Service",
    lifespan=lifespan,
)



def choose_movies_target() -> str:
    if not GRADUAL_MIGRATION:
        return MOVIES_SERVICE_URL

    if random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
        return MOVIES_SERVICE_URL

    return MONOLITH_URL


def choose_target(path: str) -> str:
    if path == "/api/movies/health":
        return MOVIES_SERVICE_URL

    if path == "/api/movies" or path.startswith("/api/movies/"):
        return choose_movies_target()

    if path == "/api/events" or path.startswith("/api/events/"):
        return EVENTS_SERVICE_URL

    return MONOLITH_URL



def prepare_request_headers(request: Request) -> dict[str, str]:
    return {
        name: value
        for name, value in request.headers.items()
        if name.lower() not in {"host", "content-length"}
    }


def prepare_response_headers(
    response: httpx.Response,
) -> dict[str, str]:
    content_type = response.headers.get("content-type")

    if content_type is None:
        return {}

    return {"content-type": content_type}

@app.get("/health")
async def health() -> dict[str, bool]:
    return {"status": True}
    
@app.api_route(
    "/{path:path}",
    methods=[
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS",
        "HEAD",
    ],
)
async def proxy_request(request: Request, path: str) -> Response:
    target = choose_target(request.url.path)
    target_url = f"{target}{request.url.path}"

    if request.url.query:
        target_url = f"{target_url}?{request.url.query}"

    try:
        upstream_response = await request.app.state.http_client.request(
            method=request.method,
            url=target_url,
            headers=prepare_request_headers(request),
            content=await request.body(),
        )
    except httpx.RequestError as error:
        return JSONResponse(
            status_code=502,
            content={
                "error": "Upstream service is unavailable",
                "details": str(error),
            },
        )

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers=prepare_response_headers(upstream_response),
    )