import os
from pathlib import Path
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from experiments.data_generator import load_dataset_from_csv

from .models import Base, Customer, Product, Sale

DEFAULT_DATABASE_URL = "postgresql+psycopg2://test_user:test_pass@localhost:5432/test_db"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
DATA_SCALE = os.getenv("DATA_SCALE", "small")


def load_dataset(session, scale: str) -> None:
    dataset = load_dataset_from_csv(scale=scale)
    session.add_all([Product(**row) for row in dataset.products])
    session.add_all([Customer(**row) for row in dataset.customers])
    session.add_all([Sale(**row) for row in dataset.sales])


def drop_views(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP VIEW IF EXISTS monthly_sales;"))


@pytest.fixture(scope="function")
def engine():
    engine = create_engine(DATABASE_URL, echo=False, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="function")
def db_session(engine):
    drop_views(engine)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    factory = scoped_session(sessionmaker(bind=engine, autoflush=False))
    session = factory()
    try:
        load_dataset(session, DATA_SCALE)
        session.commit()
        yield session
    finally:
        session.rollback()
        factory.remove()
        drop_views(engine)
        Base.metadata.drop_all(engine)
