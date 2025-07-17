from datetime import datetime
from typing import TYPE_CHECKING, Optional
from uuid import uuid4

from sqlalchemy import DateTime, Text, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.sqlalchemy_schemas.utils import Base

if TYPE_CHECKING:
    from .daily import DailyTable
    from .monthly import MonthlyTable


class FileHashTable(Base):
    """SQLAlchemy model for file hash table"""

    __tablename__ = "file_hash_table"

    uuid: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4, nullable=False
    )
    sha_256_hash: Mapped[str] = mapped_column(
        Text, nullable=False, index=True, comment="SHA-256 hash of the file"
    )
    uploaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    org_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)

    # Relationships
    monthly_records: Mapped[list["MonthlyTable"]] = relationship(
        back_populates="file", cascade="all, delete-orphan", collection_class=list
    )
    daily_records: Mapped[list["DailyTable"]] = relationship(
        back_populates="file", cascade="all, delete-orphan", collection_class=list
    )
