import logging
import re
import threading
import time
import uuid
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
    get_system_ram,
    get_system_vram,
)


class ResourceExhaustedError(Exception):
    pass


@dataclass
class User:
    name: str = "Unnamed"
    token: str = uuid.uuid4().hex
    can_cold_start: bool = True
    can_access_logs: bool = False
    whitelist: List[str] = field(default_factory=list)


@dataclass
class ManagerSettings:
    update_interval: float = 5.0
    users: List[User] = field(default_factory=list)


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


class ServiceStatus(Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    STARTING = "starting"
    STOPPING = "stopping"


@dataclass
class Service:
    config: ContainerConfig
    name: str
    container_name: str
    container_id: str = ""
    pid: int = -1

    status: ServiceStatus = ServiceStatus.STOPPED
    device: int = -1

    ram: int = 0
    vram: int = 0

    # TODO: Peak is a weak metric, use moving average as well, use a class for this
    peak_vram: int = 0
    peak_ram: int = 0
    peak_boot_time: float = 0

    connections: int = 0  # TODO: Thread safe
    last_activity: float = 0

    ports: Dict[int, int] = field(default_factory=dict)

    @property
    def primary_port(self) -> int:
        return self.ports[self.config.ports[0]]

    @property
    def idle_time(self) -> float:
        return time.time() - self.last_activity

    @property
    def shutdown_cost(self) -> float:
        """Returns the cost shut down this container."""
        return (
            self.peak_boot_time
            / (self.vram + self.ram * 0.25)
            * max(0.0, 1.0 - self.idle_time / self.config.idle_timeout) ** 2
            * (10 if self.connections > 0 else 1)
        )


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
    def __init__(
        self,
        settings_path: Path = Path("settings.yaml"),
        config_path: Path = Path("services.yaml"),
    ) -> None:
        self.logger = logging.getLogger("ServiceManager")

        self.docker_client = docker.from_env()

        self.lock = threading.Lock()

        self.settings: ManagerSettings = ManagerSettings()
        self.services: dict[str, Service] = {}

        # Define container configurations
        self.last_settings_reload = 0
        self.last_config_reload = 0
        self.settings_path = settings_path
        self.config_path = config_path
        self.reload_settings_config()
        self.reload_services_config()

        # Register existing containers
        self.refresh_containers()

        self.updater = threading.Thread(target=self.update_loop, daemon=True)
        self.updater.start()

    def start_service(self, name: str) -> None:
        """
        Starts a container with the given name, waits for a slot to be available.
        :param name: The container to start.
        """
        service = self.services[name]
        if service.status != ServiceStatus.STOPPED:
            return  # TODO: Not safe
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

    def stop_services(self, services: list[Service]) -> None:
        """Stops a list of containers."""
        for service in services:
            service.status = ServiceStatus.STOPPING
        for service in services:
            self.stop_service(service.name)

    def stop_service(self, name: str) -> None:
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
            service.status = ServiceStatus.STOPPED
            service.pid = -1

    def update_loop(self) -> None:
        """
        The main update loop, refreshes the state of all containers and their memory usage.
        """
        while True:
            self.refresh_containers()
            self.refresh_container_memory()

            self.reload_services_config()
            self.reload_settings_config()

            self.cleanup_containers()

            time.sleep(self.settings.update_interval)

    def refresh_containers(self) -> None:
        """
        Refreshes the state of all containers.
        """
        with self.lock:
            name_to_service = {
                service.container_name: service for service in self.services.values()
            }
            for container in self.docker_client.containers.list():
                if container.name in name_to_service:
                    service = name_to_service[container.name]
                    service.container_id = str(container.id)
                    service.pid = container.attrs["State"]["Pid"]

                    if (
                        service.status == ServiceStatus.STARTING
                        and container.status
                        not in (
                            "running",
                            "restarting",
                            "created",
                        )
                    ):
                        # The container does not seem to be restarting
                        self.stop_service(service.name)
                    elif (
                        service.status == ServiceStatus.RUNNING
                        and container.status != "running"
                    ):
                        # It seems the container is no longer running
                        self.stop_service(service.name)
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
                if service.pid >= 0:
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
                service.status == ServiceStatus.RUNNING
                and service.connections == 0
                and time.time() - service.last_activity > service.config.idle_timeout
            ):
                self.stop_service(service.name)

    def allocate(
        self,
        uses_cpu: bool = True,
        uses_gpu: bool = True,
        required_ram: int = 0,
        required_vram: int = 0,
    ) -> int:
        """
        Searches for a free device, shutting down containers if necessary.
        :param uses_cpu: Can run on CPU.
        :param uses_gpu: Can run on GPU.
        :param required_ram: Required RAM.
        :param required_vram: Required VRAM.
        :return: The device ID, or -1 for CPU.
        :raises ResourceExhaustedError: If no device is available.
        """

        system_ram = get_system_ram()
        system_vram = get_system_vram()

        if uses_gpu:
            # First, check if a GPU is free as it is
            for gpu, memory_info in system_vram.items():
                if memory_info.free > required_vram:
                    return gpu

            # If not, for each device, check the average age of container to be closed
            costs = {gpu: 0.0 for gpu in system_vram}
            services_to_be_killed = {gpu: [] for gpu in system_vram}
            for gpu, memory_info in system_vram.items():
                if memory_info.total >= required_vram:
                    services = self.list_services(gpu)

                    acc_ram = 0
                    acc_vram = 0
                    for service in services:
                        costs[gpu] += service.shutdown_cost
                        acc_ram += service.ram
                        acc_vram += service.vram
                        services_to_be_killed[gpu].append(service)

                        # Stop when enough services are found
                        if (
                            memory_info.free + acc_vram >= required_vram
                            and system_ram.free + acc_ram
                        ):
                            break

            smallest_cost = min(list(costs.values()))
            for gpu, memory_info in system_vram.items():
                if costs[gpu] == smallest_cost:
                    self.stop_services(services_to_be_killed[gpu])
                    return gpu

        if uses_cpu:
            # First check if CPU fits as it is
            if system_ram.free > required_ram:
                raise ResourceExhaustedError()

            # Otherwise shut down services
            services = self.list_services(-1)

            services_to_be_killed = []
            acc_ram = 0
            for service in services:
                acc_ram += service.ram
                services_to_be_killed.append(service)

                # Stop when enough services are found
                if system_ram.free + acc_ram:
                    break

            self.stop_services(services_to_be_killed)

        raise ResourceExhaustedError()

    def list_services(self, device: int) -> list[Service]:
        return sorted(
            [
                service
                for service in self.services.values()
                if service.status == ServiceStatus.RUNNING and service.device == device
            ],
            key=lambda service: service.shutdown_cost,
        )

    def reload_settings_config(self) -> None:
        modtime = self.settings_path.lstat().st_mtime
        if modtime != self.last_settings_reload:
            self.last_settings_reload = modtime

            # noinspection PyBroadException
            try:
                with self.config_path.open("r") as file:
                    self.settings = from_dict(
                        ManagerSettings,
                        yaml.safe_load(file),
                        config=Config(cast=[Enum]),
                    )

                self.logger.info("Settings config reloaded.")
            except Exception:
                self.logger.exception("Failed to reload settings file.")

    def reload_services_config(self) -> None:
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

                # Shutdown all containers and mark for full re-initialization
                for name in self.services.keys():
                    if (
                        name not in configs
                        or name in self.services
                        and self.services[name].config != configs[name]
                    ):
                        self.stop_service(name)
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
