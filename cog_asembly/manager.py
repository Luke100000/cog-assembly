import logging
import re
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from functools import cache
from pathlib import Path
from typing import Dict, Optional, List

import docker
import docker.errors
import requests
import yaml
from dacite import from_dict, Config
from docker.models.containers import Container
from docker.types import DeviceRequest

from cog_asembly.utils import (
    get_process_ram,
    get_process_vram,
    get_child_processes,
    find_unused_port,
)


class HealthCheckMode(Enum):
    HTTP = "http"
    LOG = "log"
    NONE = "none"


@dataclass
class HealthCheckConfig:
    timeout: int = 5
    mode: HealthCheckMode = HealthCheckMode.HTTP
    url: str = ""
    regex: str = ""


@dataclass
class ContainerConfig:
    image: str

    max_vram: Optional[str] = None
    max_ram: Optional[str] = None
    reserve_vram: Optional[str] = None
    reserve_ram: Optional[str] = None

    recommends_gpu: bool = True
    requires_gpu: bool = False

    max_boot_time: int = 60
    idle_timeout: int = 3600
    health_check: HealthCheckConfig = HealthCheckConfig()

    ports: List[int] = field(default_factory=list)
    volumes: Dict[str, str] = field(default_factory=dict)
    environment: Dict[str, str] = field(default_factory=dict)
    cpuset_cpus: Optional[str] = None


@dataclass
class Service:
    config: ContainerConfig
    name: str
    container_name: str
    container_id: str = ""
    pid: int = -1

    running: bool = False  # TODO: Maybe change to status

    ram: int = 0
    vram: int = 0
    peak_vram: int = 0
    peak_ram: int = 0
    peak_boot_time: float = 0

    connections: int = 0  # TODO: Thread safe
    last_activity: float = 0

    ports: Dict[int, int] = field(default_factory=dict)

    @property
    def primary_port(self) -> int:
        return self.ports[self.config.ports[0]]


@cache
def to_container_name(tag: str) -> str:
    name = "ca_" + tag.lower()
    name = re.sub(r"[^a-z0-9_.-]", "-", name)
    name = re.sub(r"^[-.]+|[-.]+$", "", name)
    return name[:255]


def check_container_health(container: Container, service: Service) -> bool:
    """
    Checks the health of a container.
    :param container: The container to check.
    :param service: The service to check.
    :return: True if the container is healthy.
    """
    health_check = service.config.health_check

    if health_check.mode == HealthCheckMode.NONE:
        return True
    elif health_check.mode == HealthCheckMode.HTTP:
        try:
            # noinspection HttpUrlsUsage
            response = requests.get(
                f"http://127.0.0.1:{service.primary_port}/{health_check.url}",
                timeout=health_check.timeout,
            )
            if health_check.regex == "":
                return True
            return re.search(health_check.regex, response.text) is not None
        except requests.ConnectionError:
            return False
        except requests.Timeout:
            return False
    elif health_check.mode == HealthCheckMode.LOG:
        try:
            logs = container.logs().decode("utf-8")
            if health_check.regex == "":
                return len(logs) > 0
            return re.search(health_check.regex, logs) is not None
        except docker.errors.NotFound or docker.errors.NullResource:
            return False
    return False


class ServiceManager:
    def __init__(self, config_path: Path = Path("services.yaml")) -> None:
        self.logger = logging.getLogger("ServiceManager")

        self.docker_client = docker.from_env()

        self.lock = threading.Lock()

        self.services: dict[str, Service] = {}

        # Define container configurations
        self.last_config_reload = 0
        self.config_path = config_path
        self.reload_services_config()

        # Register existing containers
        self.refresh_containers()

        self.updater = threading.Thread(target=self.update_loop, daemon=True)
        self.updater.start()

    def start_container(self, name: str) -> None:
        """
        Starts a container with the given name, waits for a slot to be available.
        :param name: The container to start.
        """
        service = self.services[name]
        if service.running:
            return
        with self.lock:
            try:
                container = self.docker_client.containers.get(service.container_name)
                container.remove(force=True)
                self.logger.info("Old container '%s' has been deleted.", name)
            except docker.errors.NotFound:
                pass

            service.ports = {port: find_unused_port() for port in service.config.ports}

            container = self.docker_client.containers.run(
                image=service.config.image,
                name=service.container_name,
                detach=True,
                mem_limit=service.config.max_ram,
                cpuset_cpus=service.config.cpuset_cpus,  # TODO: Verify hyper-threading
                ports={
                    f"{docker_port}/tcp": host_port
                    for docker_port, host_port in service.ports.items()
                },
                device_requests=[
                    DeviceRequest(device_ids=["0"], capabilities=[["gpu"]])
                ],
            )  # TODO: Port can be none, use it

        self.refresh_containers()
        self.logger.info("Starting container %s", name)

        # Wait for the container to boot
        t = time.time()
        while time.time() - t < service.config.max_boot_time:
            if check_container_health(container, service):
                boot_time = time.time() - t
                self.logger.info(
                    "Container '%s' ready after %.1f seconds.", name, boot_time
                )
                service.peak_boot_time = max(service.peak_boot_time, boot_time)
                return

            time.sleep(max(0.1, min(1.0, service.config.max_boot_time / 10)))

        self.logger.warning("Container '%s' seems to be stuck.", name)

    def stop_container(self, name: str) -> None:
        """
        Shuts down a container, waiting for all connections to close.
        :param name: The container to stop.
        """
        self.logger.info("Stopping container %s", name)

        while self.services[name].connections > 0:
            self.logger.info(
                "Waiting for %s connections on %s to close.",
                self.services[name].connections,
                name,
            )
            time.sleep(1)

        with self.lock:
            service = self.services[name]
            # noinspection PyBroadException
            try:
                self.docker_client.containers.get(service.container_id).stop()
            except Exception:
                self.logger.exception("Failed to stop container %s", name)
            service.running = False
            service.pid = -1

    def update_loop(self) -> None:
        """
        The main update loop, refreshes the state of all containers and their memory usage.
        """
        while True:
            self.refresh_containers()
            self.refresh_container_memory()
            self.cleanup_containers()
            self.reload_services_config()
            time.sleep(5)

    def refresh_containers(self) -> None:
        """
        Refreshes the state of all containers.
        """
        with self.lock:
            for service in self.services.values():
                service.running = False
            name_to_service = {
                service.container_name: service for service in self.services.values()
            }
            for container in self.docker_client.containers.list():
                if container.name in name_to_service:
                    service = name_to_service[container.name]
                    service.container_id = str(container.id)
                    service.pid = container.attrs["State"]["Pid"]
                    service.running = container.status in (
                        "running",
                        "restarting",
                        "created",
                    )
                elif container.name.startswith("ca_"):
                    container.remove(force=True)

    def refresh_container_memory(self) -> None:
        """
        Refreshes the memory usage of all containers.
        """
        ram = get_process_ram()
        vram = get_process_vram()
        with self.lock:
            for service in self.services.values():
                if service.pid >= 0 and service.running:
                    all_pids = [service.pid] + get_child_processes(service.pid)
                    service.ram = max(ram.get(pid, 0) for pid in all_pids)
                    service.vram = sum(vram.get(pid, 0) for pid in all_pids)
                    service.peak_ram = max(service.peak_ram, service.ram)
                    service.peak_vram = max(service.peak_vram, service.vram)
                else:
                    service.ram = 0
                    service.vram = 0

    def cleanup_containers(self) -> None:
        """
        Cleans up all containers that have been idle for too long.
        """
        for service in self.services.values():
            if (
                service.running
                and service.connections == 0
                and time.time() - service.last_activity > service.config.idle_timeout
            ):
                self.stop_container(service.name)

    def reload_services_config(self):
        modtime = self.config_path.lstat().st_mtime
        if modtime != self.last_config_reload:
            self.last_config_reload = modtime

            # noinspection PyBroadException
            try:
                with self.config_path.open("r") as file:
                    configs = {
                        name: from_dict(
                            ContainerConfig, config, config=Config(cast=[Enum])
                        )
                        for name, config in yaml.safe_load(file).items()
                    }

                # Shutdown all containers
                for name in self.services.keys():
                    if (
                        name not in configs
                        or name in self.services
                        and self.services[name].config != configs[name]
                    ):
                        self.services[name].running = False  # TODO: Add status!
                        self.stop_container(name)
                        del self.services[name]

                # Init new or changed services
                for name, config in configs.items():
                    self.services[name] = Service(
                        config=config,
                        name=name,
                        container_name=to_container_name(name),
                        ports={p: -1 for p in config.ports},
                    )

                self.logger.info("Services config reloaded.")
            except Exception:
                self.logger.exception("Failed to reload config file.")
