from collections import defaultdict
from decimal import Decimal

from .query import (
    s1_monthly_revenue,
    s2_category_revenue,
    s4_revenue_checks,
    s6_topn_per_customer,
)


def test_s1_monthly_sum_equals_total(db_session):
    monthly, total = s1_monthly_revenue(db_session)
    assert total is not None
    monthly_sum = sum((row_total or Decimal("0.00")) for _, row_total in monthly)
    assert monthly_sum == total


def test_s2_category_sum_equals_total(db_session):
    by_category, total = s2_category_revenue(db_session)
    assert total is not None
    category_sum = sum((row_total or Decimal("0.00")) for _, row_total in by_category)
    assert category_sum == total


def test_s4_revenue_not_null_and_non_negative(db_session):
    null_count, total_revenue = s4_revenue_checks(db_session)
    assert null_count == 0
    assert total_revenue is not None
    assert total_revenue >= 0


def test_s6_topn_per_customer(db_session):
    results = s6_topn_per_customer(db_session, n=3)
    by_customer: dict[int, list[tuple[int, Decimal, int]]] = defaultdict(list)
    for customer_id, sale_id, revenue, rn in results:
        assert revenue is not None
        by_customer[customer_id].append((sale_id, revenue, rn))

    for rows in by_customer.values():
        assert len(rows) <= 3
        for idx, (sale_id, revenue, rn) in enumerate(rows, start=1):
            assert rn == idx
            if idx > 1:
                prev_sale_id, prev_revenue, _ = rows[idx - 2]
                assert prev_revenue > revenue or (
                    prev_revenue == revenue and prev_sale_id < sale_id
                )
