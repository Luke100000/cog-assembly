import socket
from dataclasses import dataclass
from typing import Union

import psutil
import pynvml
from cachetools import cached, TTLCache

MIN_POLL_INTERVAL = 1


@dataclass
class MemoryInfo:
    free: int
    used: int
    total: int


@cached(TTLCache(maxsize=1, ttl=MIN_POLL_INTERVAL))
def get_system_vram() -> dict[int, MemoryInfo]:
    """
    Get system VRAM for each GPU
    """
    device_count = pynvml.nvmlDeviceGetCount()
    devices = {}
    for device_id in range(device_count):
        handle = pynvml.nvmlDeviceGetHandleByIndex(device_id)
        memory_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
        devices[device_id] = MemoryInfo(
            free=int(memory_info.free),
            used=int(memory_info.used),
            total=int(memory_info.total),
        )

    return devices


@cached(TTLCache(maxsize=1, ttl=MIN_POLL_INTERVAL))
def get_system_ram() -> MemoryInfo:
    """
    Get system RAM usage and capacity
    """
    memory = psutil.virtual_memory()
    return MemoryInfo(
        free=memory.available,
        used=memory.used,
        total=memory.total,
    )


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


def convert_to_int(size_str: Union[str, int]) -> int:
    units = {
        "k": 10**3,
        "K": 10**3,
        "m": 10**6,
        "M": 10**6,
        "g": 10**9,
        "G": 10**9,
        "t": 10**12,
        "T": 10**12,
        "Ki": 2**10,
        "Mi": 2**20,
        "Gi": 2**30,
    }
    if isinstance(size_str, int):
        return size_str
    if size_str[-2:] in units:
        return int(float(size_str[:-2]) * units[size_str[-2:]])
    elif size_str[-1] in units:
        return int(float(size_str[:-1]) * units[size_str[-1]])
    return int(size_str)
