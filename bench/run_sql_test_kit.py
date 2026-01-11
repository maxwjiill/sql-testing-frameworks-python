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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["S1", "S2", "S4", "S6"], required=True)
    parser.add_argument("--n", type=int, default=30)
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--scale", choices=["small", "big"], default="big")
    parser.add_argument("--out-dir", default="data/output")
    return parser.parse_args()


def invoke_sql_test_kit_run(
    repo_root: Path,
    raw_path: Path,
    log_path: Path,
    scenario: str,
    iteration: int,
    phase: str,
) -> None:
    start_time = time.perf_counter()
    
    with open(log_path, "a", encoding="utf-8") as log_file:
        write_log_header(log_file, "sql_test_kit", scenario, iteration, phase)
        
        result = subprocess.run(
            [
                sys.executable,
                str(repo_root / "experiments" / "sql_test_kit_sales_aggregation" / "main_test.py"),
            ],
            cwd=repo_root,
            check=False,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    
    elapsed_ms = int((time.perf_counter() - start_time) * 1000)
    write_run_row(raw_path, "sql_test_kit", scenario, iteration, phase, elapsed_ms, result.returncode)
    
    if result.returncode != 0:
        raise RuntimeError(f"sql-test-kit failed ({phase}/{iteration})")


def main() -> None:
    args = parse_args()
    os.chdir(REPO_ROOT)
    
    os.environ["DATA_SCALE"] = args.scale
    os.environ["SCENARIO"] = args.scenario
    
    raw_path = REPO_ROOT / args.out_dir / "raw_runs.csv"
    ensure_results_file(raw_path)
    
    log_path = get_log_file_path("sql_test_kit", args.scenario)
    
    try:
        ensure_postgres_container_running()
        
        for i in range(1, args.warmup + 1):
            invoke_sql_test_kit_run(REPO_ROOT, raw_path, log_path, args.scenario, i, "warmup")
        
        for i in range(1, args.n + 1):
            invoke_sql_test_kit_run(REPO_ROOT, raw_path, log_path, args.scenario, i, "measured")
    finally:
        stop_postgres_container()


if __name__ == "__main__":
    main()
