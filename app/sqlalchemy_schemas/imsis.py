from typing import TYPE_CHECKING, Optional
from uuid import uuid4

from sqlalchemy import ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ..sqlalchemy_schemas.utils import Base

if TYPE_CHECKING:
    from .daily import DailyTable
    from .monthly import MonthlyTable


class IMSISTable(Base):
    """SQLAlchemy model for IMSIS table"""

    __tablename__ = "imsis_table"

    uuid: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4, nullable=False
    )
    daily_uuid: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("daily_table.uuid", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    monthly_uuid: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("monthly_table.uuid", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    imsi: Mapped[str] = mapped_column(
        Text,  # IMSI is typically 15 digits
        nullable=False,
        index=True,
    )

    # Relationships
    daily_record: Mapped[Optional["DailyTable"]] = relationship(
        back_populates="imsis_records"
    )
    monthly_record: Mapped[Optional["MonthlyTable"]] = relationship(
        back_populates="imsis_records"
    )
