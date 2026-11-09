"""Opt-in retry demo against two disposable local containers."""

import os
import secrets
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest


@contextmanager
def running_container(docker, image, port, data_dir, settings, health):
    env_args = [arg for key in settings for arg in ("--env", key)]
    container = docker(
        "create", "--pull=never", "--tmpfs", data_dir, "-p", f"127.0.0.1::{port}",
        "--hostname", "retry-e2e", "--health-cmd", health, "--health-interval", "1s",
        "--health-timeout", "5s", *env_args, image,
    )
    try:
        docker("start", container)
        deadline = time.monotonic() + 90
        while True:
            status = docker("inspect", "--format", "{{.State.Status}} {{.State.Health.Status}}", container)
            if status == "running healthy":
                break
            if not status.startswith("running ") or time.monotonic() >= deadline:
                pytest.fail(f"{image} startup: {status}; delivery_id=None, last_state=None")
            time.sleep(0.5)
        binding = docker("port", container, f"{port}/tcp")
        host, published_port = binding.rsplit(":", 1)
        assert host == "127.0.0.1" and published_port.isdecimal(), binding
        yield published_port
    finally:
        docker("rm", "--force", "--volumes", container)


@pytest.mark.skipif(
    os.getenv("RUN_RETRY_E2E") != "1", reason="set RUN_RETRY_E2E=1 to run the retry demo"
)
def test_fail_once_delivery_succeeds_on_retry(tmp_path: Path) -> None:
    # The application starts in a fresh process and directory with only test settings.
    env = {key: os.environ[key] for key in ("PATH", "SystemRoot", "TEMP", "TMP") if key in os.environ}
    env.update(
        POSTGRES_HOST="127.0.0.1", POSTGRES_DB="retry_e2e", POSTGRES_USER="retry_e2e",
        POSTGRES_PASSWORD=secrets.token_hex(16),
        RABBITMQ_HOST="127.0.0.1", RABBITMQ_DEFAULT_VHOST="retry_e2e",
        RABBITMQ_DEFAULT_USER="retry_e2e", RABBITMQ_DEFAULT_PASS=secrets.token_hex(16),
        NO_PROXY="*",
    )
    docker_host = "npipe:////./pipe/docker_engine" if sys.platform == "win32" else "unix:///var/run/docker.sock"

    def docker(*args: str) -> str:
        return subprocess.run(
            ["docker", "--config", str(tmp_path), "--host", docker_host, *args],
            cwd=tmp_path, env=env, check=True, timeout=30, stdout=subprocess.PIPE, text=True,
        ).stdout.strip()

    with (
        running_container(
            docker, "postgres:18.4", 5432, "/var/lib/postgresql",
            ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"),
            "pg_isready -h 127.0.0.1 -U retry_e2e -d retry_e2e",
        ) as db_port,
        running_container(
            docker, "rabbitmq:4.3.4-management", 5672, "/var/lib/rabbitmq",
            ("RABBITMQ_DEFAULT_USER", "RABBITMQ_DEFAULT_PASS", "RABBITMQ_DEFAULT_VHOST"),
            "bash -c 'exec 3<>/dev/tcp/127.0.0.1/5672'",
        ) as broker_port,
    ):
        scenario = Path(__file__).with_name("retry_scenario.py")
        subprocess.run(
            [sys.executable, "-I", "-B", "-u", str(scenario), db_port, broker_port],
            cwd=tmp_path, env=env, check=True, timeout=150,
        )
