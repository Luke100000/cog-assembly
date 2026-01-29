import hashlib
import logging
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from functools import cache
from pathlib import Path
from typing import Optional, List

import docker
import docker.errors
import humanize
import requests
from docker.models.containers import Container
from docker.types import DeviceRequest, Mount

from app.database import SessionLocal
from app.models import ServiceModel
from app.utils import (
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


class HealthCheckMode(Enum):
    HTTP = "http"
    LOG = "log"
    NONE = "none"


@dataclass
class ServiceConfig:
    image: str

    max_vram: Optional[str] = None
    max_ram: Optional[str] = None

    use_gpu: bool = True
    use_cpu: bool = True

    max_boot_time: int = 60
    idle_timeout: int = 3600

    # Health check fields
    health_check_type: str = "none"  # none, http, log
    health_check_url: str = ""
    health_check_regex: str = ""

    # Container configuration
    port: int = 6000
    mounts: str = ""  # multi-line string
    environment: str = ""  # multi-line string
    cpuset_cpus: Optional[str] = None

    # Permission group required to access this service (empty = public)
    permission_group: str = ""


@dataclass
class User:
    token: str = uuid.uuid4().hex
    groups: List[str] = field(default_factory=list)

    def has_group(self, group: str) -> bool:
        """Check if user has a specific permission group. Admin has access to everything."""
        return "admin" in self.groups or group in self.groups


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

    host_port: int = -1

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
    if service.config.health_check_type == HealthCheckMode.NONE.value:
        return True
    elif service.config.health_check_type == HealthCheckMode.HTTP.value:
        try:
            response = requests.get(
                f"http://127.0.0.1:{service.host_port}/{service.config.health_check_url}",
                timeout=30,
            )
            if not service.config.health_check_regex:
                return True

            return (
                re.search(service.config.health_check_regex, response.text) is not None
            )
        except requests.ConnectionError:
            return False
        except requests.Timeout:
            return False
    elif service.config.health_check_type == HealthCheckMode.LOG.value:
        try:
            logs = container.logs().decode("utf-8")
            if not service.config.health_check_regex:
                return len(logs) > 0
            return re.search(service.config.health_check_regex, logs) is not None
        except docker.errors.NotFound or docker.errors.NullResource:
            return False
    return False


class ServiceManager:
    def __init__(self, config_path: Path = Path("config.yaml")) -> None:
        self.logger = logging.getLogger("ServiceManager")

        self.docker_client = docker.from_env()

        self.lock = threading.Lock()

        self.services: dict[str, Service] = {}

        # Define container configurations
        self.last_settings_reload = 0
        self.last_config_reload = 0
        self.config_path = config_path

        # Register existing containers
        self.refresh_services()
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
            self.logger.warning("A stopping service was accessed! %s", name)
            while service.status == ServiceStatus.STOPPING:
                time.sleep(0.1)

        # Container is already running
        if service.status == ServiceStatus.RUNNING:
            return

        # Unexpected state
        if service.status != ServiceStatus.STOPPED:
            self.logger.warning(
                "Unexpected service state %s for %s, should be stopped",
                service.status,
                name,
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

            # Find free port
            service.host_port = find_unused_port()

            # Parse environment and mounts
            docker_mounts = []
            for m in parse_mounts_multiline(service.config.mounts):
                if ":" in m:
                    docker_mounts.append(Mount.parse_mount_string(m))
                else:
                    docker_mounts.append(
                        Mount(
                            target=m,
                            source=f"ca_{service.container_name}_{hashlib.md5(m.encode()).hexdigest()}",
                            type="volume",
                        )
                    )

            container = self.docker_client.containers.run(
                image=service.config.image,
                name=service.container_name,
                detach=True,
                mem_limit=service.config.max_ram,
                cpuset_cpus=service.config.cpuset_cpus,
                ports={f"{service.config.port}/tcp": service.host_port},
                device_requests=[
                    DeviceRequest(
                        device_ids=[str(service.device)], capabilities=[["gpu"]]
                    )
                ]
                if service.device >= 0
                else [],
                mounts=docker_mounts,
                environment=parse_env_multiline(service.config.environment),
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
            self.refresh_services()

            self.refresh_containers()
            self.refresh_container_memory()

            self.cleanup_containers()

            time.sleep(5)

    def refresh_services(self) -> None:
        session = SessionLocal()
        try:
            db_names = {name for (name,) in session.query(ServiceModel.name).all()}

            # remove services no longer in database
            for name in set(self.services) - db_names:
                self.stop_service(name)
                del self.services[name]

            # add missing services
            missing = db_names - set(self.services)
            if not missing:
                return

            rows = (
                session.query(ServiceModel).filter(ServiceModel.name.in_(missing)).all()
            )

            for db in rows:
                self.services[db.name] = Service(
                    name=db.name,
                    container_name=to_container_name(db.name),
                    config=ServiceConfig(
                        image=db.image,
                        max_vram=db.max_vram,
                        max_ram=db.max_ram,
                        use_gpu=db.use_gpu,
                        use_cpu=db.use_cpu,
                        max_boot_time=db.max_boot_time or 60,
                        idle_timeout=db.idle_timeout or 3600,
                        health_check_type=db.health_check_type or "none",
                        health_check_url=db.health_check_url or "",
                        health_check_regex=db.health_check_regex or "",
                        port=db.port,
                        mounts=db.mounts or "",
                        environment=db.environment or "",
                        cpuset_cpus=db.cpuset_cpus,
                        permission_group=db.permission_group or "",
                    ),
                )
        finally:
            session.close()

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
                    not in {
                        "running",
                        "restarting",
                        "created",
                    }
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
                # Unknown container
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
                for service in services_to_be_killed[device]:
                    self.stop_service(service.name)
                return device

        raise ResourceExhaustedError()

    def list_services(self, device: int) -> list[Service]:
        """List services in memory."""
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


def parse_env_multiline(env_str: str) -> dict:
    env = {}
    for line in env_str.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def parse_mounts_multiline(mounts_str: str) -> list:
    mounts = []
    for line in mounts_str.splitlines():
        line = line.strip()
        if line:
            mounts.append(line)
    return mounts
