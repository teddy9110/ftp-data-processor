from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..utils.utils import normalize_to_first_of_month


class MonthlyBase(BaseModel):
    """Base model for monthly data validation"""

    model_config = ConfigDict(from_attributes=True)

    file_uuid: UUID
    service_uuid: UUID
    commitment_uuid: UUID
    tap_uuid: UUID
    date: datetime = Field(..., description="Date normalized to 1st of month")
    volume: float
    service_type: str
    vpmn: str
    hpmn: str


class MonthlyCreate(MonthlyBase):
    """Model for creating monthly records"""

    @field_validator("date")
    @classmethod
    def normalize_date(cls, v: datetime) -> datetime:
        return normalize_to_first_of_month(v)


class Monthly(MonthlyBase):
    """Model for monthly data with all fields"""

    uuid: UUID
