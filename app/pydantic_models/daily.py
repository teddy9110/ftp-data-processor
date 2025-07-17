from datetime import date
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class DailyBase(BaseModel):
    """Base model for daily data validation"""

    model_config = ConfigDict(from_attributes=True)

    file_uuid: UUID
    service_uuid: UUID
    commitment_uuid: UUID
    tap_uuid: UUID
    date: date
    volume: Decimal


class DailyCreate(DailyBase):
    """Model for creating daily records"""

    pass


class Daily(DailyBase):
    """Model for daily data with all fields"""

    uuid: UUID
