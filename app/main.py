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
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import PlainTextResponse, StreamingResponse

from app.admin import setup_admin
from app.database import init_db, SessionLocal, create_default_user_if_empty
from app.models import UserModel
from app.manager import ServiceManager, ServiceStatus, User
from app.metrics import start_metrics
from app.password import get_or_create_secret_key
from app.utils import get_system_ram, get_system_vram

logging.basicConfig(level=logging.INFO)

init_db()
create_default_user_if_empty()

pynvml.nvmlInit()

manager = ServiceManager()

# Get or create secret key
SECRET_KEY = get_or_create_secret_key()

# Static public user for unauthenticated requests
PUBLIC_USER = User(
    token="public",
    groups=[],
)


# noinspection PyUnusedLocal
@asynccontextmanager
async def lifespan(_: FastAPI):
    start_metrics(manager)
    yield
    pynvml.nvmlShutdown()


app = FastAPI(lifespan=lifespan)

# Add session middleware for admin authentication
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# Setup SQLAdmin
setup_admin(app, SECRET_KEY)

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
) -> User:
    # No credentials = public user (no permissions)
    if credentials is None:
        return PUBLIC_USER

    if credentials.scheme != "Bearer":
        raise HTTPException(401, "Invalid authentication scheme")

    # Query user by token
    session = SessionLocal()
    try:
        db_user = (
            session.query(UserModel).filter_by(token=credentials.credentials).first()
        )
        if not db_user:
            raise HTTPException(401, "Invalid token")

        return User(
            token=db_user.token,
            groups=db_user.get_groups_list(),
        )
    finally:
        session.close()


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

    service = manager.services[name]

    # Check permissions
    if service.config.permission_group and not user.has_group(
        service.config.permission_group
    ):
        raise HTTPException(
            403,
            f"You do not have access to this service (requires group: {service.config.permission_group})",
        )

    service.connections += 1
    service.last_activity = time.time()

    manager.start_service(name)

    try:
        # Forward the request
        response = requests.request(
            request.method,
            url=f"http://127.0.0.1:{service.host_port}/{path}",
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
        raise HTTPException(404, "Unknown error")

    finally:
        service.connections -= 1


@app.get("/log/{path:path}")
def get_log(
    path: str,
    user: User = Depends(get_user),
):
    if path not in manager.services:
        raise HTTPException(404, "Container not found")

    if not user.has_group("admin"):
        raise HTTPException(
            403, "You do not have access to logs (requires admin group)"
        )

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
    if not user.has_group("stats"):
        raise HTTPException(
            403, "You do not have access to stats (requires stats or admin group)"
        )

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
    uvicorn.run("main:app", reload=True)
