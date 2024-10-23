import socket

import psutil
import pynvml
from cachetools import cached, TTLCache

MIN_POLL_INTERVAL = 1


@cached(TTLCache(maxsize=1, ttl=MIN_POLL_INTERVAL))
def get_system_vram() -> dict:
    """
    Get system VRAM for each GPU
    """
    device_count = pynvml.nvmlDeviceGetCount()
    devices = {}
    for device_id in range(device_count):
        handle = pynvml.nvmlDeviceGetHandleByIndex(device_id)
        memory_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
        devices[device_id] = {
            "free": memory_info.free,
            "used": memory_info.used,
            "total": memory_info.total,
        }

    return devices


@cached(TTLCache(maxsize=1, ttl=MIN_POLL_INTERVAL))
def get_system_ram() -> dict:
    """
    Get system RAM usage and capacity
    """
    memory = psutil.virtual_memory()
    return {
        "free": memory.available,
        "used": memory.used,
        "total": memory.total,
    }


@cached(TTLCache(maxsize=1, ttl=MIN_POLL_INTERVAL))
def get_process_vram() -> dict[int, int]:
    """
    Get VRAM usage for all running processes
    :return: A mapping between PID and VRAM usage.
    """
    device_count = pynvml.nvmlDeviceGetCount()
    processes = {}
    for device_id in range(device_count):
        handle = pynvml.nvmlDeviceGetHandleByIndex(device_id)
        for process in pynvml.nvmlDeviceGetComputeRunningProcesses(handle):
            processes[process.pid] = process.usedGpuMemory
            # todo and all its children (or other way around, follow up until a docker is found)
    return processes


@cached(TTLCache(maxsize=1, ttl=MIN_POLL_INTERVAL))
def get_process_ram() -> dict[int, int]:
    """
    Get memory info for all running processes
    :return: A mapping between PID and info.
    """
    processes = {}
    for proc in psutil.process_iter(["pid", "memory_info"]):
        try:
            ram = proc.info["memory_info"].rss
            if ram:
                processes[proc.info["pid"]] = ram
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return processes


def get_child_processes(pid: int) -> list[int]:
    """Return a list of all child PIDs for a given PID."""
    try:
        parent = psutil.Process(pid)
        return [child.pid for child in parent.children(recursive=True)]
    except psutil.NoSuchProcess:
        return []


def find_unused_port(start_port: int = 1024, end_port: int = 65535) -> int:
    for port in range(start_port, end_port + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("localhost", port)) != 0:
                return port
    raise RuntimeError("No unused ports available in the specified range.")
