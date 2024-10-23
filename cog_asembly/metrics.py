import threading

from prometheus_client import Gauge

from cog_asembly.manager import ServiceManager, ServiceStatus
from cog_asembly.utils import (
    get_system_ram,
    get_system_vram,
)

# Prometheus Gauges for system metrics
ram_total = Gauge("ram_total_bytes", "Total system RAM")
ram_used = Gauge("ram_used_bytes", "Used system RAM")
ram_free = Gauge("ram_free_bytes", "Free system RAM")

vram_total = Gauge("vram_total_bytes", "Total system VRAM", ["gpu"])
vram_used = Gauge("vram_used_bytes", "Used system VRAM", ["gpu"])
vram_free = Gauge("vram_free_bytes", "Free system VRAM", ["gpu"])

# Service-specific metrics
service_peak_ram = Gauge(
    "service_peak_ram_bytes", "Peak RAM usage of service", ["service_name"]
)
service_peak_vram = Gauge(
    "service_peak_vram_bytes", "Peak VRAM usage of service", ["service_name"]
)
service_boot_time = Gauge(
    "service_boot_time_seconds", "Service boot time", ["service_name"]
)
service_current_ram = Gauge(
    "service_current_ram_bytes", "Current RAM usage of service", ["service_name"]
)
service_current_vram = Gauge(
    "service_current_vram_bytes", "Current VRAM usage of service", ["service_name"]
)
service_connections = Gauge(
    "service_current_connections",
    "Current active connections of service",
    ["service_name"],
)
service_running = Gauge(
    "service_running",
    "Service running state (1 = running, 0 = not running)",
    ["service_name", "status"],
)


def update_metrics(manager: ServiceManager):
    # Update system RAM metrics
    ram = get_system_ram()
    ram_total.set(ram.total)
    ram_used.set(ram.used)
    ram_free.set(ram.free)

    # Update system VRAM metrics
    vram = get_system_vram()
    for gpu, data in vram.items():
        vram_total.labels(gpu=gpu).set(data.total)
        vram_used.labels(gpu=gpu).set(data.used)
        vram_free.labels(gpu=gpu).set(data.free)

    # Update service metrics
    for service in manager.services.values():
        service_peak_ram.labels(service_name=service.name).set(service.peak_ram)
        service_peak_vram.labels(service_name=service.name).set(service.peak_vram)
        service_boot_time.labels(service_name=service.name).set(service.peak_boot_time)
        service_current_ram.labels(service_name=service.name).set(service.ram)
        service_current_vram.labels(service_name=service.name).set(service.vram)
        service_connections.labels(service_name=service.name).set(service.connections)

        for status in ServiceStatus:
            service_running.labels(service_name=service.name, status=status.value).set(
                1 if service.status == status else 0
            )


def start_metrics(manager: ServiceManager):
    threading.Thread(target=update_metrics, args=(manager,)).start()
