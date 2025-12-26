from __future__ import annotations

import csv
import math
from pathlib import Path
from statistics import median


RAW_PATH = Path("data") / "output" / "raw_runs.csv"
SUMMARY_PATH = Path("data") / "output" / "summary.csv"


def percentile_nearest_rank(values: list[float], p: float) -> float:
    """Nearest-rank percentile, p in [0,1]."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    rank = max(1, math.ceil(p * len(sorted_vals)))
    return sorted_vals[rank - 1]


def main() -> None:
    if not RAW_PATH.exists():
        raise SystemExit(f"Missing {RAW_PATH}")

    groups: dict[tuple[str, str], list[float]] = {}

    with RAW_PATH.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("phase") != "measured":
                continue
            if row.get("exit_code") != "0":
                continue
            tool = row.get("tool", "")
            scenario = row.get("scenario", "")
            try:
                duration = float(row.get("duration_ms", "0"))
            except ValueError:
                continue
            groups.setdefault((tool, scenario), []).append(duration)

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for (tool, scenario), values in sorted(groups.items()):
        n = len(values)
        mean = sum(values) / n if n else 0.0
        if n > 1:
            variance = sum((v - mean) ** 2 for v in values) / (n - 1)
        else:
            variance = 0.0
        std = math.sqrt(variance)
        min_v = min(values) if values else 0.0
        max_v = max(values) if values else 0.0
        median_v = median(values) if values else 0.0
        p95_v = percentile_nearest_rank(values, 0.95) if values else 0.0
        cv = std / mean if mean > 0 else 0.0

        rows.append(
            {
                "tool": tool,
                "scenario": scenario,
                "n": n,
                "mean_ms": f"{mean:.3f}",
                "variance_ms2": f"{variance:.3f}",
                "std_ms": f"{std:.3f}",
                "min_ms": f"{min_v:.3f}",
                "median_ms": f"{median_v:.3f}",
                "p95_ms": f"{p95_v:.3f}",
                "max_ms": f"{max_v:.3f}",
                "cv": f"{cv:.6f}",
            }
        )

    with SUMMARY_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "tool",
                "scenario",
                "n",
                "mean_ms",
                "variance_ms2",
                "std_ms",
                "min_ms",
                "median_ms",
                "p95_ms",
                "max_ms",
                "cv",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    if rows:
        headers = [
            "tool",
            "scenario",
            "n",
            "mean_ms",
            "variance_ms2",
            "std_ms",
            "min_ms",
            "median_ms",
            "p95_ms",
            "max_ms",
            "cv",
        ]
        print("| " + " | ".join(headers) + " |")
        print("| " + " | ".join(["---"] * len(headers)) + " |")
        for row in rows:
            print("| " + " | ".join(str(row[h]) for h in headers) + " |")


if __name__ == "__main__":
    main()
