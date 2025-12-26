from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """SQLAlchemy metadata base."""


class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False)

    def __repr__(self) -> str:
        return (
            f"Product(product_id={self.product_id!r}, category={self.category!r}, "
            f"active={self.active!r})"
        )


class Customer(Base):
    __tablename__ = "customers"

    customer_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    segment: Mapped[str] = mapped_column(String(50), nullable=False)

    def __repr__(self) -> str:
        return f"Customer(customer_id={self.customer_id!r}, segment={self.segment!r})"


class Sale(Base):
    __tablename__ = "sales"

    sale_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sale_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    customer_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("customers.customer_id"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("products.product_id"), nullable=False
    )
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    discount: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)

    def __repr__(self) -> str:
        return (
            f"Sale(sale_id={self.sale_id!r}, sale_ts={self.sale_ts!r}, "
            f"customer_id={self.customer_id!r}, product_id={self.product_id!r}, "
            f"qty={self.qty!r}, price={self.price!r}, discount={self.discount!r})"
        )
