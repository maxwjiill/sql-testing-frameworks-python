from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from pathlib import Path
import os
import sys

import pandas as pd
import psycopg2
from sql_test_kit.column import Column
from sql_test_kit.query_interpolation import (
    InterpolationData,
    replace_table_names_in_string_by_data_literals,
)
from sql_test_kit.table import Table

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from experiments.data_generator import DDL_STATEMENTS, DROP_STATEMENTS, resolve_input_dir


DB_CFG = {
    "host": "localhost",
    "port": 5432,
    "user": "test_user",
    "password": "test_pass",
    "dbname": "test_db",
}

DATA_SCALE = os.getenv("DATA_SCALE", "small")
SCENARIO = os.getenv("SCENARIO", "").strip().upper()
REVENUE_EXPR = "s.qty * COALESCE(s.price, 0) * (1 - COALESCE(s.discount, 0))"


def build_dataframes():
    input_dir = resolve_input_dir(DATA_SCALE)

    customers_df = pd.read_csv(input_dir / "customers.csv", dtype=object)
    products_df = pd.read_csv(input_dir / "products.csv", dtype=object)
    sales_df = pd.read_csv(input_dir / "sales.csv", dtype=object)

    customers_df = customers_df.where(pd.notnull(customers_df), None)
    products_df = products_df.where(pd.notnull(products_df), None)
    sales_df = sales_df.where(pd.notnull(sales_df), None)

    return customers_df, products_df, sales_df


def build_tables():
    customers_table = Table(
        table_path="customers",
        columns=[Column("customer_id", "INT"), Column("segment", "TEXT")],
    )
    products_table = Table(
        table_path="products",
        columns=[
            Column("product_id", "INT"),
            Column("category", "TEXT"),
            Column("active", "BOOLEAN"),
        ],
    )
    sales_table = Table(
        table_path="sales",
        columns=[
            Column("sale_id", "INT"),
            Column("sale_ts", "TIMESTAMP"),
            Column("customer_id", "INT"),
            Column("product_id", "INT"),
            Column("qty", "INT"),
            Column("price", "NUMERIC(10,2)"),
            Column("discount", "NUMERIC(5,2)"),
        ],
    )
    return customers_table, products_table, sales_table


def _insert_dataframe(cur, table_name: str, df: pd.DataFrame, table: Table) -> None:
    chunk_size = 1000
    columns = ", ".join(df.columns)
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size]
        select_sql = replace_table_names_in_string_by_data_literals(
            f"SELECT * FROM {table_name} AS data",
            [InterpolationData(table=table, data=chunk)],
        )
        select_sql = select_sql.replace('"', "'")
        select_sql = select_sql.replace("'None'", "NULL").replace("'nan'", "NULL")
        insert_sql = f"INSERT INTO {table_name} ({columns}) {select_sql};"
        cur.execute(insert_sql)


def prepare_database(conn) -> None:
    customers_df, products_df, sales_df = build_dataframes()
    customers_table, products_table, sales_table = build_tables()

    with conn.cursor() as cur:
        for stmt in DROP_STATEMENTS:
            cur.execute(stmt)
        for stmt in DDL_STATEMENTS:
            cur.execute(stmt)
        _insert_dataframe(cur, "customers", customers_df, customers_table)
        _insert_dataframe(cur, "products", products_df, products_table)
        _insert_dataframe(cur, "sales", sales_df, sales_table)


def s1_monthly_sum_equals_total(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DATE_TRUNC('month', s.sale_ts)::date AS month, SUM({REVENUE_EXPR}) AS total
            FROM sales s
            GROUP BY month
            ORDER BY month;
            """
        )
        monthly = cur.fetchall()
        cur.execute(f"SELECT SUM({REVENUE_EXPR}) AS total FROM sales s;")
        total = cur.fetchone()[0]

    monthly_sum = sum((row[1] or Decimal("0.00")) for row in monthly)
    assert total is not None
    assert monthly_sum == total


def s2_category_sum_equals_total(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT p.category, SUM({REVENUE_EXPR}) AS total
            FROM sales s
            JOIN products p ON p.product_id = s.product_id
            GROUP BY p.category
            ORDER BY p.category;
            """
        )
        by_category = cur.fetchall()
        cur.execute(f"SELECT SUM({REVENUE_EXPR}) AS total FROM sales s;")
        total = cur.fetchone()[0]

    category_sum = sum((row[1] or Decimal("0.00")) for row in by_category)
    assert total is not None
    assert category_sum == total


def s4_revenue_not_null_and_non_negative(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE revenue IS NULL) AS null_count,
                SUM(revenue) AS total_revenue
            FROM (
                SELECT {REVENUE_EXPR} AS revenue
                FROM sales s
            ) t;
            """
        )
        null_count, total_revenue = cur.fetchone()

    assert null_count == 0
    assert total_revenue is not None
    assert total_revenue >= 0


def s6_topn_per_customer(conn, n: int = 3) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH ranked AS (
                SELECT
                    s.customer_id,
                    s.sale_id,
                    {REVENUE_EXPR} AS revenue,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.customer_id
                        ORDER BY {REVENUE_EXPR} DESC, s.sale_id ASC
                    ) AS rn
                FROM sales s
            )
            SELECT customer_id, sale_id, revenue, rn
            FROM ranked
            WHERE rn <= %s
            ORDER BY customer_id, rn;
            """,
            (n,),
        )
        rows = cur.fetchall()

    by_customer: dict[int, list[tuple[int, Decimal, int]]] = defaultdict(list)
    for customer_id, sale_id, revenue, rn in rows:
        assert revenue is not None
        by_customer[customer_id].append((sale_id, revenue, rn))

    for rows in by_customer.values():
        assert len(rows) <= n
        for idx, (sale_id, revenue, rn) in enumerate(rows, start=1):
            assert rn == idx
            if idx > 1:
                prev_sale_id, prev_revenue, _ = rows[idx - 2]
                assert prev_revenue > revenue or (
                    prev_revenue == revenue and prev_sale_id < sale_id
                )


def run_all_tests() -> None:
    with psycopg2.connect(**DB_CFG) as conn:
        prepare_database(conn)
        scenario_map = {
            "S1": s1_monthly_sum_equals_total,
            "S2": s2_category_sum_equals_total,
            "S4": s4_revenue_not_null_and_non_negative,
            "S6": s6_topn_per_customer,
        }
        if SCENARIO:
            scenario_fn = scenario_map.get(SCENARIO)
            if not scenario_fn:
                raise ValueError(f"Unknown scenario: {SCENARIO}")
            scenario_fn(conn)
            print(f"Scenario {SCENARIO} passed.")
        else:
            s1_monthly_sum_equals_total(conn)
            s2_category_sum_equals_total(conn)
            s4_revenue_not_null_and_non_negative(conn)
            s6_topn_per_customer(conn)
            print("All sql-test-kit checks passed.")


if __name__ == "__main__":
    run_all_tests()
