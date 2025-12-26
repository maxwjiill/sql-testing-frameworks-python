from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import csv
import random
from pathlib import Path
from typing import Iterable


DEFAULT_SEED = 42


@dataclass(frozen=True)
class DataScale:
    products: int
    customers: int
    sales: int


SMALL_SCALE = DataScale(products=50, customers=200, sales=5000)
BIG_SCALE = DataScale(products=500, customers=2000, sales=50000)


@dataclass(frozen=True)
class Dataset:
    products: list[dict]
    customers: list[dict]
    sales: list[dict]


CATEGORIES = ["electronics", "home", "sports", "books", "toys", "beauty"]
SEGMENTS = ["consumer", "corporate", "small_business"]


DDL_STATEMENTS = [
    """
    CREATE TABLE customers(
        customer_id INT PRIMARY KEY,
        segment TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE products(
        product_id INT PRIMARY KEY,
        category TEXT NOT NULL,
        active BOOLEAN NOT NULL
    );
    """,
    """
    CREATE TABLE sales(
        sale_id INT PRIMARY KEY,
        sale_ts TIMESTAMP NOT NULL,
        customer_id INT NOT NULL REFERENCES customers(customer_id),
        product_id INT NOT NULL REFERENCES products(product_id),
        qty INT NOT NULL,
        price NUMERIC(10, 2),
        discount NUMERIC(5, 2)
    );
    """,
]

DROP_STATEMENTS = [
    "DROP VIEW IF EXISTS monthly_sales;",
    "DROP TABLE IF EXISTS sales;",
    "DROP TABLE IF EXISTS products;",
    "DROP TABLE IF EXISTS customers;",
]


def get_scale(scale: str) -> DataScale:
    if scale == "small":
        return SMALL_SCALE
    if scale == "big":
        return BIG_SCALE
    raise ValueError(f"Unknown scale: {scale!r}")


def _rand_decimal(rng: random.Random, low: float, high: float) -> Decimal:
    value = Decimal(str(rng.uniform(low, high)))
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _format_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def generate_dataset(scale: str = "small", seed: int = DEFAULT_SEED) -> Dataset:
    cfg = get_scale(scale)
    rng = random.Random(seed)

    products = []
    for product_id in range(1, cfg.products + 1):
        products.append(
            {
                "product_id": product_id,
                "category": rng.choice(CATEGORIES),
                "active": rng.random() < 0.85,
            }
        )

    customers = []
    for customer_id in range(1, cfg.customers + 1):
        customers.append({"customer_id": customer_id, "segment": rng.choice(SEGMENTS)})

    start = datetime(2023, 1, 1)
    days_in_year = 365

    def build_sale(row_id: int, customer_id: int) -> dict:
        day_offset = rng.randint(0, days_in_year - 1)
        sec_offset = rng.randint(0, 24 * 60 * 60 - 1)
        sale_ts = start + timedelta(days=day_offset, seconds=sec_offset)
        qty = rng.randint(1, 5)
        price = None if rng.random() < 0.05 else _rand_decimal(rng, 5, 500)
        discount = None if rng.random() < 0.15 else _rand_decimal(rng, 0, 0.3)
        return {
            "sale_id": row_id,
            "sale_ts": sale_ts,
            "customer_id": customer_id,
            "product_id": rng.randint(1, cfg.products),
            "qty": qty,
            "price": price,
            "discount": discount,
        }

    sales = []
    sale_id = 1
    for customer_id in range(1, cfg.customers + 1):
        for _ in range(2):
            sales.append(build_sale(sale_id, customer_id))
            sale_id += 1

    remaining = cfg.sales - len(sales)
    for _ in range(remaining):
        customer_id = rng.randint(1, cfg.customers)
        sales.append(build_sale(sale_id, customer_id))
        sale_id += 1

    return Dataset(products=products, customers=customers, sales=sales)


def dataset_to_csv_rows(dataset: Dataset) -> dict[str, list[dict]]:
    def serialize_sales(rows: Iterable[dict]) -> list[dict]:
        output = []
        for row in rows:
            output.append(
                {
                    "sale_id": row["sale_id"],
                    "sale_ts": row["sale_ts"].strftime("%Y-%m-%d %H:%M:%S"),
                    "customer_id": row["customer_id"],
                    "product_id": row["product_id"],
                    "qty": row["qty"],
                    "price": _format_decimal(row["price"]),
                    "discount": _format_decimal(row["discount"]),
                }
            )
        return output

    def serialize_products(rows: Iterable[dict]) -> list[dict]:
        return [
            {
                "product_id": row["product_id"],
                "category": row["category"],
                "active": "true" if row["active"] else "false",
            }
            for row in rows
        ]

    def serialize_customers(rows: Iterable[dict]) -> list[dict]:
        return [
            {"customer_id": row["customer_id"], "segment": row["segment"]}
            for row in rows
        ]

    return {
        "sales": serialize_sales(dataset.sales),
        "products": serialize_products(dataset.products),
        "customers": serialize_customers(dataset.customers),
    }


def resolve_input_dir(scale: str) -> Path:
    root = Path(__file__).resolve().parents[2] / "data" / "input"
    candidate = root / scale
    return candidate if candidate.is_dir() else root


def _parse_decimal(value: str) -> Decimal | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    return Decimal(value)


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"true", "1", "t", "yes", "y"}


def load_dataset_from_csv(scale: str = "small") -> Dataset:
    input_dir = resolve_input_dir(scale)

    def read_csv(path: Path) -> list[dict]:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    customers_rows = read_csv(input_dir / "customers.csv")
    products_rows = read_csv(input_dir / "products.csv")
    sales_rows = read_csv(input_dir / "sales.csv")

    customers = [
        {"customer_id": int(row["customer_id"]), "segment": row["segment"]}
        for row in customers_rows
    ]
    products = [
        {
            "product_id": int(row["product_id"]),
            "category": row["category"],
            "active": _parse_bool(row["active"]),
        }
        for row in products_rows
    ]
    sales = [
        {
            "sale_id": int(row["sale_id"]),
            "sale_ts": datetime.strptime(row["sale_ts"], "%Y-%m-%d %H:%M:%S"),
            "customer_id": int(row["customer_id"]),
            "product_id": int(row["product_id"]),
            "qty": int(row["qty"]),
            "price": _parse_decimal(row["price"]),
            "discount": _parse_decimal(row["discount"]),
        }
        for row in sales_rows
    ]

    return Dataset(products=products, customers=customers, sales=sales)
