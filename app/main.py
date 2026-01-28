import logging
import time
from contextlib import asynccontextmanager
from typing import Dict, Optional

import docker
import docker.errors
import pynvml
import requests
import uvicorn
from fastapi import FastAPI
from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from humanize import naturalsize
from prometheus_client import make_asgi_app
from pydantic import BaseModel
from starlette.background import BackgroundTask
from starlette.responses import PlainTextResponse, StreamingResponse

from app.manager import ServiceManager, ServiceStatus, User
from app.metrics import start_metrics
from app.utils import get_system_ram, get_system_vram

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


def get_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(
        HTTPBearer(auto_error=False)
    ),
):
    if credentials is None:
        return manager.config.users.get("default")
    if credentials.scheme != "Bearer" or credentials.credentials not in manager.tokens:
        raise HTTPException(401, "Invalid token")
    return manager.tokens[credentials.credentials]


@app.api_route("/c/{name}/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
def proxy_request(
    name: str,
    path: str,
    request: Request,
    body: bytes = Depends(get_body),
    user: User = Depends(get_user),
):
    if name not in manager.services:
        raise HTTPException(404, "Container not found")

    if user.whitelist and name not in user.whitelist:
        raise HTTPException(403, "You do not have access to this service")

    service = manager.services[name]

    service.connections += 1
    service.last_activity = time.time()

    manager.start_service(name)

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
            stream=True,
        )

        return StreamingResponse(
            response.iter_content(chunk_size=1),
            status_code=response.status_code,
            headers=response.headers,
            background=BackgroundTask(response.close),
        )

    except Exception:
        logging.exception("Proxy request for %s failed", name)
        raise HTTPException(
            404, "Unknown error, are you sure this endpoint is correct?"
        )

    finally:
        service.connections -= 1


@app.get("/log/{path:path}")
def get_log(
    path: str,
    user: User = Depends(get_user),
):
    if path not in manager.services:
        raise HTTPException(404, "Container not found")

    if not user.can_access_logs:
        raise HTTPException(403, "You do not have access to logs")

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
def stats(user: User = Depends(get_user)):
    if not user.can_access_stats:
        raise HTTPException(403, "You do not have access to stats")

    ram = get_system_ram()
    return PlainTextResponse(
        "\n".join(
            [
                f"System RAM: {naturalsize(ram.used)} of {naturalsize(ram.total)} used ({ram.used / ram.total:.1%}), {manager.allocated_memory(-1) / ram.total:.1%} allocated to services.",
                *[
                    f"GPU {gpu}: {naturalsize(vram.used)} of {naturalsize(vram.total)} used ({vram.used / vram.total:.1%}), {manager.allocated_memory(gpu) / vram.total:.1%} allocated to services."
                    for gpu, vram in get_system_vram().items()
                ],
                "",
                f"{sum([1 for service in manager.services.values() if service.status == ServiceStatus.RUNNING])} of {len(manager.services)} services running:",
                *[
                    f"- {service.name}: {service.status.value}, {naturalsize(service.ram)} RAM, {naturalsize(service.vram)} VRAM, {service.connections} connections"
                    if service.status != ServiceStatus.STOPPED
                    else f"- {service.name}: {service.status.value}"
                    for service in manager.services.values()
                ],
            ],
        )
    )


if __name__ == "__main__":
    uvicorn.run(app)
