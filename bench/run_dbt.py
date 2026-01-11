from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
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

SELECT_MAP = {
    "S1": "tag:S1",
    "S2": "tag:S2",
    "S4": "tag:S4",
    "S6": "tag:S6",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["S1", "S2", "S4", "S6"], required=True)
    parser.add_argument("--n", type=int, default=30)
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--scale", choices=["small", "big"], default="big")
    parser.add_argument("--out-dir", default="data/output")
    return parser.parse_args()


def invoke_dbt_run(
    repo_root: Path,
    raw_path: Path,
    log_path: Path,
    scenario: str,
    select_arg: str,
    iteration: int,
    phase: str,
) -> None:
    start_time = time.perf_counter()
    dbt_dir = repo_root / "experiments" / "dbt_sales_aggregation"
    exit_code = 0
    
    with open(log_path, "a", encoding="utf-8") as log_file:
        write_log_header(log_file, "dbt", scenario, iteration, phase)
        
        result = subprocess.run(
            [sys.executable, str(repo_root / "experiments" / "dbt_sales_aggregation" / "generate_seeds.py")],
            cwd=repo_root,
            check=False,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        exit_code = result.returncode
        
        if exit_code == 0:
            result = subprocess.run(
                ["dbt", "seed", "--no-use-colors", "--full-refresh", "--project-dir", str(dbt_dir)],
                cwd=repo_root,
                check=False,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            exit_code = result.returncode
        
        if exit_code == 0:
            result = subprocess.run(
                ["dbt", "run", "--no-use-colors", "--project-dir", str(dbt_dir)],
                cwd=repo_root,
                check=False,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            exit_code = result.returncode
        
        if exit_code == 0:
            cmd = ["dbt", "test", "--no-use-colors", "--project-dir", str(dbt_dir)]
            if select_arg:
                cmd.extend(["--select", select_arg])
            result = subprocess.run(cmd, cwd=repo_root, check=False, stdout=log_file, stderr=subprocess.STDOUT)
            exit_code = result.returncode
    
    elapsed_ms = int((time.perf_counter() - start_time) * 1000)
    write_run_row(raw_path, "dbt", scenario, iteration, phase, elapsed_ms, exit_code)
    
    if exit_code != 0:
        raise RuntimeError(f"dbt failed for {scenario} ({phase}/{iteration})")


def main() -> None:
    args = parse_args()
    os.chdir(REPO_ROOT)
    
    os.environ["DATA_SCALE"] = args.scale
    os.environ["DBT_PROFILES_DIR"] = str(REPO_ROOT / "experiments" / "dbt_sales_aggregation" / "profiles")
    os.environ["DBT_USE_COLORS"] = "false"
    
    raw_path = REPO_ROOT / args.out_dir / "raw_runs.csv"
    ensure_results_file(raw_path)
    
    log_path = get_log_file_path("dbt", args.scenario)
    select_arg = SELECT_MAP[args.scenario]
    dbt_dir = REPO_ROOT / "experiments" / "dbt_sales_aggregation"
    
    try:
        ensure_postgres_container_running()
        
        with open(log_path, "a", encoding="utf-8") as log_file:
            log_file.write(f"dbt deps started at {datetime.now().isoformat()}\n")
            log_file.flush()
            result = subprocess.run(
                ["dbt", "deps", "--no-use-colors", "--project-dir", str(dbt_dir)],
                cwd=REPO_ROOT,
                check=True,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
        
        for i in range(1, args.warmup + 1):
            invoke_dbt_run(REPO_ROOT, raw_path, log_path, args.scenario, select_arg, i, "warmup")
        
        for i in range(1, args.n + 1):
            invoke_dbt_run(REPO_ROOT, raw_path, log_path, args.scenario, select_arg, i, "measured")
    finally:
        stop_postgres_container()


if __name__ == "__main__":
    main()
