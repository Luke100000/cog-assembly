import logging
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from functools import cache
from pathlib import Path
from typing import Dict, Optional, List, Any, Union

import docker
import docker.errors
import humanize
import requests
import yaml
from dacite import from_dict, Config
from docker.models.containers import Container
from docker.types import DeviceRequest, Mount

from cog_asembly.utils import (
    get_process_ram,
    get_process_vram,
    get_child_processes,
    find_unused_port,
    get_system_ram,
    get_system_vram,
    convert_to_int,
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
class MountConfig:
    target: str
    source: str
    type: str = "volume"
    read_only: bool = False
    consistency: Optional[str] = None
    propagation: Optional[str] = None
    no_copy: bool = False
    labels: Optional[Dict[str, Any]] = None
    driver_config: Optional[Any] = None
    tmpfs_size: Optional[Union[int, str]] = None
    tmpfs_mode: Optional[int] = None


@dataclass
class ServiceConfig:
    image: str
    public: bool = True

    max_vram: Optional[str] = None
    max_ram: Optional[str] = None

    use_gpu: bool = True
    use_cpu: bool = True

    max_boot_time: int = 60
    idle_timeout: int = 3600

    health_check: HealthCheckConfig = HealthCheckConfig()

    # Container configuration
    ports: List[int] = field(default_factory=list)
    mounts: List[MountConfig] = field(default_factory=list)
    environment: Dict[str, str] = field(default_factory=dict)
    cpuset_cpus: Optional[str] = None


class ServiceStatus(Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    STARTING = "starting"
    STOPPING = "stopping"


@dataclass
class Service:
    config: ServiceConfig
    name: str
    container_name: str
    container_id: str = ""
    pid: int = -1

    status: ServiceStatus = ServiceStatus.STOPPED
    device: int = -1

    ram: int = 0
    vram: int = 0
    boot_time: float = 0
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
            max(1.0, self.boot_time)
            / (self.vram + self.ram * 0.25 + 10**8)
            * max(0.0, 1.0 - self.idle_time / self.config.idle_timeout) ** 2
            * (10 if self.connections > 0 else 1)
            * (1 if self.config.use_gpu else 0.5)
        )

    @property
    def reserved_ram(self) -> int:
        return max(self.ram, convert_to_int(self.config.max_ram or 0))

    @property
    def reserved_vram(self) -> int:
        return max(self.vram, convert_to_int(self.config.max_vram or 0))


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

        # Container is already booting, await state change
        if service.status == ServiceStatus.STARTING:
            t = time.time()
            while (
                service.status == ServiceStatus.STARTING
                and time.time() - t < service.config.max_boot_time
            ):
                time.sleep(0.1)
            return

        # Container is stopping, wait for it to stop, then restart
        if service.status == ServiceStatus.STOPPING:
            self.logger.warning("A stopping service was accessed!", name)
            while service.status == ServiceStatus.STOPPING:
                time.sleep(0.1)

        # Container is already running
        if service.status == ServiceStatus.RUNNING:
            return

        # Unexpected state
        if service.status != ServiceStatus.STOPPED:
            self.logger.warning(
                "Unexpected service state %s, should be stopped", service.status
            )
            return

        # Look for a free device
        service.device = self.allocate(
            use_cpu=service.config.use_cpu,
            use_gpu=service.config.use_gpu,
            required_ram=convert_to_int(service.config.max_ram or service.peak_ram),
            required_vram=convert_to_int(service.config.max_vram or service.peak_vram),
        )

        self.logger.info("Starting container %s on device %s", name, service.device)
        service.status = ServiceStatus.STARTING

        with self.lock:
            try:
                container = self.docker_client.containers.get(service.container_name)
                container.remove(force=True)
                self.logger.info("Old container '%s' has been deleted.", name)
            except docker.errors.NotFound:
                pass

            # Find free ports
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
                    DeviceRequest(
                        device_ids=[str(service.device)], capabilities=[["gpu"]]
                    )
                ]
                if service.device >= 0
                else [],
                mounts=[
                    Mount(
                        target=m.target,
                        source=m.source,
                        type=m.type,
                        read_only=m.read_only,
                        consistency=m.consistency,
                        propagation=m.propagation,
                        no_copy=m.no_copy,
                        labels=m.labels,
                        driver_config=m.driver_config,
                        tmpfs_size=m.tmpfs_size,
                        tmpfs_mode=m.tmpfs_mode,
                    )
                    for m in service.config.mounts
                ],
                environment=service.config.environment,
            )

        # Sanity check and retrieve container ID
        self.refresh_containers()

        # Wait for the container to boot
        t = time.time()
        while time.time() - t < service.config.max_boot_time:
            if check_container_health(container, service):
                service.boot_time = time.time() - t
                if service.boot_time > service.peak_boot_time:
                    service.peak_boot_time = service.boot_time
                    if service.boot_time > service.config.max_boot_time:
                        self.logger.warning(
                            "Container '%s' took %.1f seconds instead of estimated %d to boot.",
                            name,
                            service.boot_time,
                            service.config.max_boot_time,
                        )
                service.status = ServiceStatus.RUNNING
                self.logger.info(
                    "Container '%s' ready after %.1f seconds.", name, service.boot_time
                )
                return

            time.sleep(0.1)

        self.logger.warning("Container '%s' seems to be stuck.", name)

    def stop_services(self, services: list[Service]) -> None:
        """Stops a list of containers."""
        for service in services:
            self.stop_service(service.name)

    def stop_service(self, name: str) -> None:
        """
        Shuts down a container, waiting for all connections to close.
        :param name: The container to stop.
        """
        service = self.services[name]

        if service.status == ServiceStatus.STOPPING:
            self.logger.info("Service %s is already stopping.", name)
            while service.status == ServiceStatus.STOPPING:
                time.sleep(0.1)

        if service.status == ServiceStatus.STARTING:
            self.logger.warning("Attempting to stop service %s while starting.", name)
            while service.status == ServiceStatus.STARTING:
                time.sleep(0.1)

        if service.status == ServiceStatus.STOPPED:
            return

        self.logger.info("Stopping container %s", name)

        # Mark as stopping to prevent new connections
        service.status = ServiceStatus.STOPPING

        # Wait for all connections to close
        while service.connections > 0:
            self.logger.info(
                "Waiting for %s connections on %s to close.",
                service.connections,
                name,
            )
            time.sleep(1)

        with self.lock:
            service = service
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
                elif service.status == ServiceStatus.STOPPED:
                    # Container should not be running
                    service.status = ServiceStatus.RUNNING
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

                    if service.ram > service.peak_ram:
                        service.peak_ram = service.ram
                        if service.ram > convert_to_int(service.config.max_ram or 0):
                            self.logger.warning(
                                "Container '%s' exceeded estimated RAM usage: %s > %s",
                                service.name,
                                humanize.naturalsize(service.ram),
                                humanize.naturalsize(service.peak_ram),
                            )

                    if service.vram > service.peak_vram:
                        service.peak_vram = service.vram
                        if service.vram > convert_to_int(service.config.max_vram or 0):
                            self.logger.warning(
                                "Container '%s' exceeded estimated VRAM usage: %s > %s",
                                service.name,
                                humanize.naturalsize(service.vram),
                                humanize.naturalsize(service.peak_vram),
                            )
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
        use_cpu: bool = True,
        use_gpu: bool = True,
        required_ram: int = 0,
        required_vram: int = 0,
    ) -> int:
        """
        Searches for a free device, shutting down containers if necessary.
        :param use_cpu: Can run on CPU.
        :param use_gpu: Can run on GPU.
        :param required_ram: Required RAM.
        :param required_vram: Required VRAM.
        :return: The device ID, or -1 for CPU.
        :raises ResourceExhaustedError: If no device is available.
        """

        system_ram = get_system_ram()
        system_vram = get_system_vram()

        # Usage not caused by services
        system_usage = {-1: max(0, system_ram.used - self.used_memory(-1))}
        for gpu, memory_info in system_vram.items():
            system_usage[gpu] = max(0, memory_info.used - self.used_memory(gpu))

        # List all valid devices
        valid_devices = []
        if use_gpu:
            for gpu, memory_info in system_vram.items():
                if memory_info.total - system_usage[gpu] >= required_vram:
                    valid_devices.append(gpu)
        if use_cpu:
            if system_ram.total - system_usage[-1] >= required_ram:
                valid_devices.append(-1)

        # Free memory for each device, considering reserved memory
        free_memory = {
            device: (system_ram.total if device < 0 else system_vram[device].total)
            - system_usage[device]
            - self.allocated_memory(device)
            for device in (valid_devices + [-1])
        }

        # Choose the device with the least cost to shut down
        costs = {device: 0.0 for device in valid_devices}
        services_to_be_killed = {device: [] for device in valid_devices}
        for device in valid_devices:
            services = self.list_services(device)

            acc_ram = 0
            acc_vram = 0
            for service in services:
                # Stop when enough services are found
                if (
                    free_memory[-1] + acc_ram >= required_ram
                    and free_memory[device] + acc_vram >= required_vram
                ):
                    break

                acc_ram += service.reserved_ram
                acc_vram += service.reserved_vram
                costs[device] += service.shutdown_cost
                services_to_be_killed[device].append(service)

        # Avoid CPU when possible
        if -1 in costs:
            costs[-1] += 1_000_000

        # Stop the services
        smallest_cost = min(list(costs.values()))
        for device in valid_devices:
            if costs[device] == smallest_cost:
                self.stop_services(services_to_be_killed[device])
                return device

        raise ResourceExhaustedError()

    def list_services(self, device: int) -> list[Service]:
        return sorted(
            [
                service
                for service in self.services.values()
                if service.status != ServiceStatus.STOPPED
                and (service.device == device or device < 0)
            ],
            key=lambda service: service.shutdown_cost,
        )

    def used_memory(self, device: int) -> int:
        """Returns the total allocated bytes for a device."""
        return sum(s.vram for s in self.list_services(device))

    def allocated_memory(self, device: int) -> int:
        """Returns the total allocated bytes for a device."""
        return sum(
            (s.reserved_ram if device < 0 else s.reserved_vram)
            for s in self.list_services(device)
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
                            ServiceConfig, config, config=Config(cast=[Enum])
                        )
                        for name, config in yaml.safe_load(file).items()
                    }

                # Shutdown all containers and mark for full re-initialization
                for name in list(self.services.keys()):
                    if (
                        name not in configs
                        or name in self.services
                        and self.services[name].config != configs[name]
                    ):
                        self.stop_service(name)
                        del self.services[name]

                # Init new or changed services
                for name, config in configs.items():
                    if name not in self.services:
                        self.services[name] = Service(
                            config=config,
                            name=name,
                            container_name=to_container_name(name),
                            ports={p: -1 for p in config.ports},
                        )

                self.logger.info("Services config reloaded.")
            except Exception:
                self.logger.exception("Failed to reload config file.")
