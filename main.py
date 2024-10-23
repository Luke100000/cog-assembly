import logging
import time
from contextlib import asynccontextmanager
from typing import Dict

import docker
import docker.errors
import humanize
import pynvml
import requests
from fastapi import FastAPI
from fastapi import HTTPException, Request, Depends
from prometheus_client import make_asgi_app
from pydantic import BaseModel
from starlette.responses import PlainTextResponse

from cog_asembly.manager import ServiceManager
from cog_asembly.metrics import start_metrics
from cog_asembly.utils import (
    get_system_ram,
    get_system_vram,
)

logging.basicConfig(level=logging.INFO)

pynvml.nvmlInit()
manager = ServiceManager()


# noinspection PyUnusedLocal
@asynccontextmanager
async def lifespan(_: FastAPI):
    start_metrics(manager)
    yield
    pynvml.nvmlShutdown()


app = FastAPI(lifespan=lifespan)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


# Model for request data
class ProxyRequest(BaseModel):
    data: Dict


async def get_body(request: Request):
    return await request.body()


@app.api_route("/c/{name}/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
def proxy_request(
    name: str, path: str, request: Request, body: bytes = Depends(get_body)
):
    if name not in manager.services:
        raise HTTPException(404, "Container not found")

    service = manager.services[name]

    service.connections += 1
    service.last_activity = time.time()

    manager.start_container(name)

    try:
        # Forward the request
        response = requests.request(
            request.method,
            url=f"http://127.0.0.1:{service.primary_port}/{path}",
            headers=dict(request.headers),
            # files=request.files,  # TODO
            data=body,
            json=request.json,
            params=request.query_params,
            # auth=request.auth,
            cookies=request.cookies,
        )

        return PlainTextResponse(response.content, status_code=response.status_code)

    except Exception:
        logging.exception("Proxy request for %s failed", name)
        raise HTTPException(
            404, "Unknown error, are you sure this endpoint is correct?"
        )

    finally:
        service.connections -= 1


@app.get("/log/{path:path}")
def get_log(path: str):
    if path not in manager.services:
        raise HTTPException(404, "Container not found")

    service = manager.services[path]

    try:
        return PlainTextResponse(
            manager.docker_client.containers.get(service.container_id).logs()
        )
    except docker.errors.NotFound:
        raise HTTPException(404, "Container log not found")
    except docker.errors.NullResource:
        raise HTTPException(404, "Container not found")


@app.get("/health", description="Get the overall health status of the service manager")
def stats():
    ram = get_system_ram()
    return PlainTextResponse(
        "\n".join(
            [
                f"System RAM: {humanize.naturalsize(ram['used'])} of {humanize.naturalsize(ram['total'])} used ({ram['used'] / ram['total']:.2%}).",
                *[
                    f"GPU {gpu}: {humanize.naturalsize(vram['used'])} of {humanize.naturalsize(vram['total'])} used ({vram['used'] / vram['total']:.2%})."
                    for gpu, vram in get_system_vram().items()
                ],
                f"{sum([1 for service in manager.services.values() if service.running])} of {len(manager.services)} services running.",
            ],
        )
    )
