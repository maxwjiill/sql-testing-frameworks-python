from __future__ import annotations

import csv
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import TextIO

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTAINER_NAME = "postgres_tests"
LOG_DIR = REPO_ROOT / "logs"
POSTGRES_PORT = 15432


def ensure_results_file(raw_path: Path) -> None:
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    if not raw_path.exists():
        with open(raw_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "tool", "scenario", "iteration", "phase", "duration_ms", "exit_code"])


def write_run_row(
    raw_path: Path,
    tool: str,
    scenario: str,
    iteration: int,
    phase: str,
    duration_ms: int,
    exit_code: int,
) -> None:
    timestamp = datetime.utcnow().isoformat()
    with open(raw_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, tool, scenario, iteration, phase, duration_ms, exit_code])


def remove_postgres_container() -> None:
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"name={CONTAINER_NAME}", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout.strip():
        subprocess.run(
            ["docker", "rm", "-f", CONTAINER_NAME],
            capture_output=True,
            check=False,
        )


def ensure_postgres_container_running() -> None:
    remove_postgres_container()
    
    subprocess.run(
        [
            "docker",
            "run",
            "--name", CONTAINER_NAME,
            "-e", "POSTGRES_USER=test_user",
            "-e", "POSTGRES_PASSWORD=test_pass",
            "-e", "POSTGRES_DB=test_db",
            "-p", f"{POSTGRES_PORT}:5432",
            "-d",
            "postgres:15",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    
    deadline = time.time() + 60
    while time.time() < deadline:
        result = subprocess.run(
            [
                "docker",
                "exec",
                CONTAINER_NAME,
                "psql",
                "-U", "test_user",
                "-d", "postgres",
                "-c", "SELECT 1;",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            break
        time.sleep(2)
    else:
        raise RuntimeError("Postgres container not ready within 60 seconds.")
    
    result = subprocess.run(
        [
            "docker",
            "exec",
            CONTAINER_NAME,
            "psql",
            "-U", "test_user",
            "-d", "postgres",
            "-tAc",
            "SELECT 1 FROM pg_database WHERE datname='test_db';",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    db_exists = result.stdout.strip() == "1"
    
    if not db_exists:
        subprocess.run(
            [
                "docker",
                "exec",
                CONTAINER_NAME,
                "psql",
                "-U", "test_user",
                "-d", "postgres",
                "-c", "CREATE DATABASE test_db;",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def stop_postgres_container() -> None:
    subprocess.run(
        ["docker", "stop", CONTAINER_NAME],
        capture_output=True,
        check=False,
    )


def cleanup_testcontainers() -> None:
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", "label=org.testcontainers", "--format", "{{.ID}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout.strip():
        ids = result.stdout.strip().split("\n")
        for container_id in ids:
            subprocess.run(
                ["docker", "rm", "-f", container_id],
                capture_output=True,
                check=False,
            )


def get_log_file_path(tool: str, scenario: str) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return LOG_DIR / f"{timestamp}_{tool}_{scenario}.log"


def write_log_header(log_file: TextIO, tool: str, scenario: str, iteration: int, phase: str) -> None:
    log_file.write(f"\n{'='*80}\n")
    log_file.write(f"Tool: {tool}, Scenario: {scenario}, Iteration: {iteration}, Phase: {phase}\n")
    log_file.write(f"Time: {datetime.now().isoformat()}\n")
    log_file.write(f"{'='*80}\n\n")
    log_file.flush()
