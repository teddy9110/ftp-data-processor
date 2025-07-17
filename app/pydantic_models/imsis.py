from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class IMSISBase(BaseModel):
    """Base model for IMSIS validation"""

    model_config = ConfigDict(from_attributes=True)

    daily_uuid: Optional[UUID] = None
    monthly_uuid: Optional[UUID] = None
    imsi: str = Field(..., description="IMSI identifier string")


class IMSISCreate(IMSISBase):
    """Model for creating IMSIS records"""

    pass


class IMSIS(IMSISBase):
    """Model for IMSIS with all fields"""

    uuid: UUID
