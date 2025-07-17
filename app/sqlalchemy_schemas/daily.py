from datetime import datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import DateTime, Double, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.sqlalchemy_schemas.utils import Base

if TYPE_CHECKING:
    from .file_hash import FileHashTable
    from .imsis import IMSISTable


class DailyTable(Base):
    """SQLAlchemy model for daily data table"""

    __tablename__ = "daily_table"

    uuid: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4, nullable=False
    )
    file_uuid: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("file_hash_table.uuid", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    service_uuid: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), nullable=False, index=True
    )
    commitment_uuid: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), nullable=False, index=True
    )
    tap_uuid: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), nullable=False, index=True
    )
    date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    volume: Mapped[float] = mapped_column(Double, nullable=False)

    # Relationships
    file: Mapped["FileHashTable"] = relationship(back_populates="daily_records")
    imsis_records: Mapped[list["IMSISTable"]] = relationship(
        back_populates="daily_record",
        cascade="all, delete-orphan",
        collection_class=list,
    )
