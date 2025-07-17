from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class FileHashBase(BaseModel):
    """Base model for file hash validation"""

    model_config = ConfigDict(from_attributes=True)

    sha_256_hash: str = Field(..., description="SHA-256 hash of the file")
    org_name: Optional[str] = Field(None, description="Organization name")


class FileHashCreate(FileHashBase):
    """Model for creating file hash records"""

    pass


class FileHash(FileHashBase):
    """Model for file hash with all fields"""

    uuid: UUID
    uploaded_at: datetime
