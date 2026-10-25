"""Prometheus exporter for the state and resource usage of this compose project's containers.

cAdvisor cannot resolve container names with Docker Desktop's containerd image store, so the
Docker API is queried directly. Every metric is labelled with the compose service name.
"""
import os
import socket
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Event

import docker
from docker.errors import NotFound
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, CounterMetricFamily, GaugeMetricFamily

PORT = int(os.environ.get("PORT", "9417"))
PROJECT_LABEL = "com.docker.compose.project"
SERVICE_LABEL = "com.docker.compose.service"
LABELS = ["service", "container"]


def started_at(state):
    value = state.get("StartedAt", "")
    if value.startswith("0001-"):
        return 0.0
    # Docker reports nanoseconds, which fromisoformat does not accept.
    seconds, _, fraction = value.rstrip("Z").partition(".")
    return datetime.fromisoformat(seconds + "+00:00").timestamp() + float("0." + (fraction or "0"))


class DockerCollector:
    def __init__(self):
        self.client = docker.from_env(timeout=10)
        self.pool = ThreadPoolExecutor(max_workers=8)
        own_labels = self.client.containers.get(socket.gethostname()).labels
        self.project = own_labels.get(PROJECT_LABEL)

    def stats(self, container_id):
        try:
            return self.client.api.stats(container_id, stream=False, one_shot=True)
        except NotFound:
            return None

    def collect(self):
        filters = {"label": f"{PROJECT_LABEL}={self.project}"} if self.project else {}
        containers = self.client.containers.list(all=True, filters=filters, ignore_removed=True)

        running = GaugeMetricFamily("docker_container_running", "1 if the container is running.", labels=LABELS)
        healthy = GaugeMetricFamily(
            "docker_container_healthy", "1 if the health check passes, 0 if it fails (only containers with a health check).", labels=LABELS)
        exit_code = GaugeMetricFamily("docker_container_exit_code", "Exit code of the last run.", labels=LABELS)
        restarts = GaugeMetricFamily("docker_container_restart_count", "Restarts done by the Docker restart policy.", labels=LABELS)
        oom_killed = GaugeMetricFamily("docker_container_oom_killed", "1 if the last run was killed for running out of memory.", labels=LABELS)
        started = GaugeMetricFamily("docker_container_started_at_seconds", "Start time of the last run.", labels=LABELS)
        cpu = CounterMetricFamily("docker_container_cpu_seconds", "CPU time used by the container.", labels=LABELS)
        memory = GaugeMetricFamily("docker_container_memory_bytes", "Memory used, excluding reclaimable page cache.", labels=LABELS)

        running_containers = []
        for container in containers:
            state = container.attrs["State"]
            labels = [container.labels.get(SERVICE_LABEL, container.name), container.name]
            running.add_metric(labels, 1 if state["Running"] else 0)
            exit_code.add_metric(labels, state["ExitCode"])
            restarts.add_metric(labels, container.attrs.get("RestartCount", 0))
            oom_killed.add_metric(labels, 1 if state["OOMKilled"] else 0)
            started.add_metric(labels, started_at(state))
            health = state.get("Health", {}).get("Status")
            if health in ("healthy", "unhealthy"):
                healthy.add_metric(labels, 1 if health == "healthy" else 0)
            if state["Running"]:
                running_containers.append((container.id, labels))

        for (_, labels), stats in zip(running_containers,
                                      self.pool.map(self.stats, [cid for cid, _ in running_containers])):
            if not stats or not stats.get("cpu_stats", {}).get("cpu_usage"):
                continue
            cpu.add_metric(labels, stats["cpu_stats"]["cpu_usage"]["total_usage"] / 1e9)
            mem = stats.get("memory_stats", {})
            if "usage" in mem:
                memory.add_metric(labels, mem["usage"] - mem.get("stats", {}).get("inactive_file", 0))

        yield from (running, healthy, exit_code, restarts, oom_killed, started, cpu, memory)


if __name__ == "__main__":
    REGISTRY.register(DockerCollector())
    start_http_server(PORT)
    Event().wait()
