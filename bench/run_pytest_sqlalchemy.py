from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from bench.common import (
    REPO_ROOT,
    ensure_postgres_container_running,
    ensure_results_file,
    get_log_file_path,
    stop_postgres_container,
    write_log_header,
    write_run_row,
)

SCENARIO_MAP = {
    "S1": "test_s1_monthly_sum_equals_total",
    "S2": "test_s2_category_sum_equals_total",
    "S4": "test_s4_revenue_not_null_and_non_negative",
    "S6": "test_s6_topn_per_customer",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["S1", "S2", "S4", "S6"], required=True)
    parser.add_argument("--n", type=int, default=30)
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--scale", choices=["small", "big"], default="big")
    parser.add_argument("--out-dir", default="data/output")
    return parser.parse_args()


def invoke_test_run(
    repo_root: Path,
    raw_path: Path,
    log_path: Path,
    scenario: str,
    filter_name: str,
    iteration: int,
    phase: str,
) -> None:
    start_time = time.perf_counter()
    
    with open(log_path, "a", encoding="utf-8") as log_file:
        write_log_header(log_file, "pytest_sqlalchemy", scenario, iteration, phase)
        
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                str(repo_root / "experiments" / "pytest_sqlalchemy_postgres" / "test_postgres.py"),
                "-k",
                filter_name,
            ],
            cwd=repo_root,
            check=False,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    
    elapsed_ms = int((time.perf_counter() - start_time) * 1000)
    write_run_row(raw_path, "pytest_sqlalchemy", scenario, iteration, phase, elapsed_ms, result.returncode)
    
    if result.returncode != 0:
        raise RuntimeError(f"pytest_sqlalchemy failed for {scenario} ({phase}/{iteration})")


def main() -> None:
    args = parse_args()
    os.chdir(REPO_ROOT)
    
    os.environ["DATA_SCALE"] = args.scale
    from bench.common import POSTGRES_PORT
    os.environ["DATABASE_URL"] = f"postgresql+psycopg2://test_user:test_pass@localhost:{POSTGRES_PORT}/test_db"
    
    raw_path = REPO_ROOT / args.out_dir / "raw_runs.csv"
    ensure_results_file(raw_path)
    
    log_path = get_log_file_path("pytest_sqlalchemy", args.scenario)
    filter_name = SCENARIO_MAP[args.scenario]
    
    try:
        ensure_postgres_container_running()
        
        for i in range(1, args.warmup + 1):
            invoke_test_run(REPO_ROOT, raw_path, log_path, args.scenario, filter_name, i, "warmup")
        
        for i in range(1, args.n + 1):
            invoke_test_run(REPO_ROOT, raw_path, log_path, args.scenario, filter_name, i, "measured")
    finally:
        stop_postgres_container()


if __name__ == "__main__":
    main()
