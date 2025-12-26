from __future__ import annotations

from decimal import Decimal
from typing import List, Tuple

from sqlalchemy import Date, cast, func, select
from sqlalchemy.orm import Session

from .models import Product, Sale


def revenue_expr():
    return Sale.qty * func.coalesce(Sale.price, 0) * (1 - func.coalesce(Sale.discount, 0))


def s1_monthly_revenue(session: Session) -> Tuple[List[Tuple], Decimal | None]:
    month = cast(func.date_trunc("month", Sale.sale_ts), Date).label("month")
    revenue = revenue_expr()
    monthly_stmt = (
        select(month, func.sum(revenue).label("total"))
        .group_by(month)
        .order_by(month)
    )
    total_stmt = select(func.sum(revenue).label("total"))
    monthly = session.execute(monthly_stmt).all()
    total = session.execute(total_stmt).scalar()
    return [(row.month, row.total) for row in monthly], total


def s2_category_revenue(session: Session) -> Tuple[List[Tuple], Decimal | None]:
    revenue = revenue_expr()
    stmt = (
        select(Product.category, func.sum(revenue).label("total"))
        .join(Product, Sale.product_id == Product.product_id)
        .group_by(Product.category)
        .order_by(Product.category)
    )
    total_stmt = select(func.sum(revenue).label("total"))
    by_category = session.execute(stmt).all()
    total = session.execute(total_stmt).scalar()
    return [(row.category, row.total) for row in by_category], total


def s4_revenue_checks(session: Session) -> Tuple[int, Decimal | None]:
    revenue = revenue_expr()
    stmt = select(
        func.count().filter(revenue.is_(None)).label("null_count"),
        func.sum(revenue).label("total_revenue"),
    )
    row = session.execute(stmt).one()
    return row.null_count, row.total_revenue


def s6_topn_per_customer(session: Session, n: int = 3) -> List[Tuple]:
    revenue = revenue_expr().label("revenue")
    rn = func.row_number().over(
        partition_by=Sale.customer_id,
        order_by=[revenue.desc(), Sale.sale_id.asc()],
    ).label("rn")
    ranked = select(
        Sale.customer_id,
        Sale.sale_id,
        revenue,
        rn,
    ).subquery()
    stmt = (
        select(ranked.c.customer_id, ranked.c.sale_id, ranked.c.revenue, ranked.c.rn)
        .where(ranked.c.rn <= n)
        .order_by(ranked.c.customer_id, ranked.c.rn)
    )
    result = session.execute(stmt).all()
    return [
        (row.customer_id, row.sale_id, row.revenue, row.rn)
        for row in result
    ]
