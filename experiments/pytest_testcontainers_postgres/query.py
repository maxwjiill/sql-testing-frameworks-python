from __future__ import annotations

from decimal import Decimal
from typing import List, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine


REVENUE_EXPR = "s.qty * COALESCE(s.price, 0) * (1 - COALESCE(s.discount, 0))"


def s1_monthly_revenue(engine: Engine) -> Tuple[List[Tuple], Decimal | None]:
    monthly_sql = text(
        f"""
        SELECT DATE_TRUNC('month', s.sale_ts)::date AS month, SUM({REVENUE_EXPR}) AS total
        FROM sales s
        GROUP BY month
        ORDER BY month;
        """
    )
    total_sql = text(f"SELECT SUM({REVENUE_EXPR}) AS total FROM sales s;")
    with engine.connect() as conn:
        monthly = conn.execute(monthly_sql).all()
        total = conn.execute(total_sql).scalar()
    return [(row.month, row.total) for row in monthly], total


def s2_category_revenue(engine: Engine) -> Tuple[List[Tuple], Decimal | None]:
    by_category_sql = text(
        f"""
        SELECT p.category, SUM({REVENUE_EXPR}) AS total
        FROM sales s
        JOIN products p ON p.product_id = s.product_id
        GROUP BY p.category
        ORDER BY p.category;
        """
    )
    total_sql = text(f"SELECT SUM({REVENUE_EXPR}) AS total FROM sales s;")
    with engine.connect() as conn:
        by_category = conn.execute(by_category_sql).all()
        total = conn.execute(total_sql).scalar()
    return [(row.category, row.total) for row in by_category], total


def s4_revenue_checks(engine: Engine) -> Tuple[int, Decimal | None]:
    sql = text(
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
    with engine.connect() as conn:
        row = conn.execute(sql).one()
    return row.null_count, row.total_revenue


def s6_topn_per_customer(engine: Engine, n: int = 3) -> List[Tuple]:
    sql = text(
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
        WHERE rn <= :n
        ORDER BY customer_id, rn;
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"n": n}).all()
    return [(row.customer_id, row.sale_id, row.revenue, row.rn) for row in rows]
