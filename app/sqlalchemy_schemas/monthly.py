from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from pydantic import AwareDatetime
from sqlalchemy import DateTime, Double, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.sqlalchemy_schemas.utils import Base

if TYPE_CHECKING:
    from .file_hash import FileHashTable
    from .imsis import IMSISTable


class MonthlyTable(Base):
    """SQLAlchemy model for monthly data table"""

    __tablename__ = "monthly_table"

    uuid: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4, nullable=False
    )
    file_uuid: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("file_hash_table.uuid", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    service_uuid: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True, index=True
    )
    commitment_uuid: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True, index=True
    )
    tap_uuid: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True, index=True
    )

    date: Mapped[AwareDatetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
        comment="Date normalized to 1st of month",
    )
    volume: Mapped[float] = mapped_column(Double, nullable=False)
    service_type: Mapped[str] = mapped_column(Text, nullable=False)
    hpmn: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    vpmn: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    contract_uuid: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True, index=True
    )
    # Relationships
    file: Mapped["FileHashTable"] = relationship(back_populates="monthly_records")

    imsis_records: Mapped[list["IMSISTable"]] = relationship(
        back_populates="monthly_record",
        cascade="all, delete-orphan",
        collection_class=list,
    )
