import os
from pathlib import Path
import sys
import time
import warnings

warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module=r"testcontainers\.core\.waiting_utils",
)
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module=r"testcontainers\.postgres",
)

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from testcontainers.core.wait_strategies import (
    ContainerStatusWaitStrategy,
    LogMessageWaitStrategy,
)
from testcontainers.postgres import PostgresContainer

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from experiments.data_generator import DDL_STATEMENTS, DROP_STATEMENTS, load_dataset_from_csv

DATA_SCALE = os.getenv("DATA_SCALE", "small")


class ReadyPostgresContainer(PostgresContainer):
    def __init__(
        self,
        image: str = "postgres:15",
        port: int = 5432,
        username: str = "test_user",
        password: str = "test_pass",
        dbname: str = "test_db",
        **kwargs,
    ) -> None:
        super().__init__(
            image=image, port=port, username=username, password=password, dbname=dbname, **kwargs
        )
        self.waiting_for(LogMessageWaitStrategy("database system is ready to accept connections"))

    def _connect(self) -> None:
        ContainerStatusWaitStrategy().wait_until_ready(self)
        deadline = time.time() + 30
        last_exc: Exception | None = None
        while time.time() < deadline:
            engine = create_engine(self.get_connection_url(), future=True)
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return
            except OperationalError as exc:
                last_exc = exc
                time.sleep(0.5)
            finally:
                engine.dispose()
        raise RuntimeError("Postgres container was not ready in time") from last_exc


def _load_dataset(conn, scale: str) -> None:
    dataset = load_dataset_from_csv(scale=scale)
    conn.execute(
        text(
            """
            INSERT INTO customers (customer_id, segment)
            VALUES (:customer_id, :segment)
            """
        ),
        dataset.customers,
    )
    conn.execute(
        text(
            """
            INSERT INTO products (product_id, category, active)
            VALUES (:product_id, :category, :active)
            """
        ),
        dataset.products,
    )
    conn.execute(
        text(
            """
            INSERT INTO sales
                (sale_id, sale_ts, customer_id, product_id, qty, price, discount)
            VALUES
                (:sale_id, :sale_ts, :customer_id, :product_id, :qty, :price, :discount)
            """
        ),
        dataset.sales,
    )


@pytest.fixture(scope="function")
def pg_container():
    with ReadyPostgresContainer() as container:
        yield container


@pytest.fixture(scope="function")
def engine(pg_container) -> Engine:
    engine = create_engine(pg_container.get_connection_url(), future=True)
    with engine.begin() as conn:
        for stmt in DROP_STATEMENTS:
            conn.execute(text(stmt))
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))
        _load_dataset(conn, DATA_SCALE)
    try:
        yield engine
    finally:
        engine.dispose()
