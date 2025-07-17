from enum import Enum

from pydantic import BaseModel, Field


class BaseSchema(BaseModel):
    model_config = {
        "use_enum_values": True,
        "from_attributes": True,
        # "extra": "forbid",
    }


class SystemCustomType(str, Enum):
    system = "system"
    custom = "custom"


class ContractStage(str, Enum):
    awaiting_start = "awaiting_start"  # Signed but start date still in the future
    live = "live"  # Everyone signed and Contract is live (within start and end date)
    finished = "finished"  # Contract has ended
    expired = "expired"  # Contract has expired - signatures not completed on time


class NegotiationRoleType(str, Enum):
    signatory = "signatory"
    negotiator = "negotiator"


class SortOrder(str, Enum):
    asc = "asc"
    desc = "desc"


class AlertType(str, Enum):
    NEGOTIATION_EXPIRING = "NEGOTIATION_EXPIRING"
    DEAL_FINISHING = "DEAL_FINISHING"
    OUTSTANDING_SIGNATURE = "OUTSTANDING_SIGNATURE"


"""
Ignore - for a future release
class NotificationType(str, Enum):
    PROPOSAL_RECEIVED = "PROPOSAL_RECEIVED"
    COUNTER_RECEIVED = "COUNTER_RECEIVED"
    PROPOSAL_ACCEPTED = "PROPOSAL_ACCEPTED"
    PROPOSAL_REJECTED = "PROPOSAL_REJECTED"
    NEGOTIATION_EXPIRED = "NEGOTIATION_EXPIRED"
"""


class ClearingHouseType(str, Enum):
    FCH = "FCH"
    DCH = "DCH"
    # IOT = "IOT"


class ArchivalRequest(BaseModel):
    """
    Model for archival requests.
    Used to request the archival of a contract.
    """

    archived: bool = Field(examples=[True, False])
