## About

This repository contains an experimental comparison of four approaches to testing SQL queries against PostgreSQL using the same data model, the same deterministic dataset, and the same set of SQL scenarios. The main goal is to evaluate practical trade-offs between frameworks (runtime overhead and stability of execution time) while keeping the test logic equivalent.

Compared approaches:

- **pytest + SQLAlchemy**
- **pytest + Testcontainers**
- **dbt**
- **sql-test-kit**

## Authors and contributors

- **Maxim A. Metelkin** — main author, implementation and experiments
- **Vladimir A. Parkhomenko** — advisor, minor author (Senior Lecturer)

## Project structure

- `experiments/` — implementations of the four approaches
- `bench/` — runners and result aggregation utilities
- `data/input/` — input CSV files
- `data/output/` — experiment outputs
- `logs/` — run logs (global log and per-scenario logs)

## Scenarios

All approaches implement the same four SQL scenarios:

- **S1** — monthly aggregation: sum over months equals the total sum for the year
- **S2** — JOIN + aggregation by category: sum by categories equals the total sum
- **S4** — NULL handling (COALESCE) in revenue; revenue is not NULL; total revenue is non-negative
- **S6** — window top-N (N=3) by revenue with deterministic ordering

## Data

A deterministic synthetic dataset is used.

Inputs:
- `data/input/` (CSV)

Outputs:
- `data/output/raw_runs.csv`
- `data/output/summary.csv`
