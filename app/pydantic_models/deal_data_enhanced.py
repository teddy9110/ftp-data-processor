from typing import Annotated, Literal, Optional, Union

from pydantic import Field

from .deal_data import (
    AA12_DealData,
    DataRateType,
    DirectionalData,
    FinancialCommitment,
    SmsRateType,
    TapRateService,
    Tier,
    VoiceRateType,
    VolumeCommitment,
    _BaseTieredService,
)


class EnhancedTapRateService(TapRateService):
    paid: float = Field(default=0)
    chargeable: float = Field(default=0)


class EnhancedTier(Tier):
    tier_achieved: bool = Field(default=False)
    tier_achieved_volume: float = Field(default=0)


class _EnhancedBaseTieredService(_BaseTieredService):
    """TieredService covers both Tiered and Flat Rate models:
    Flat Rate is just Tiered without any tiers - one Tier with TODO"""

    volume_achieved: float = Field(examples=[1, 60], default=0)
    tier_achieved: int = Field(default=-1)
    tiers: list[EnhancedTier] = Field()


class EnhancedTieredService(_EnhancedBaseTieredService):
    model_type: Literal["structured"]  # tiered or flat


class EnhancedBalancedService(_EnhancedBaseTieredService):
    """BalancedService inherits from TieredService,
    as the unbalanced rates can be tiered or flat rates,
    and we additionally define the balanced rates paid while the agreement is live.
    """

    model_type: Literal["balanced"]
    balanced_rate: float = Field(examples=[0.1])
    balanced_rate_unit: float = Field(examples=[1])
    balanced_rate_type: Annotated[
        str, Union[VoiceRateType, SmsRateType, DataRateType]
    ] = Field()


class EnhancedFinancialCommitment(FinancialCommitment):
    """Define an amount (e.g., 1000 -> £1,000), and rates for each service.
    The commitment is reached by a composite of all these service rates;
    e.g. £0.10 per 1 minute for voice, £0.01 per 1 SMS, £0.10 per 1.5 MB of Data.
    """

    amount: float = Field(examples=[1, 60], default=0)
    amount_achieved: float = Field(examples=[1, 60], default=0)
    committment_met: bool = amount_achieved >= amount


class EnhancedVolumeCommitment(VolumeCommitment):
    """Define a volume and volume type (e.g., (1000, minutes) -> 1,000 minutes),
    and a ServiceRate, which specifies a single service and the price paid per unit of that service
    """

    volume: int = Field(examples=[1, 60], default=0)
    volume_achieved: int = Field(examples=[1, 60], default=0)
    committment_met: bool = volume_achieved >= volume


class EnhancedDirectionalData(DirectionalData):
    commitments: list[
        Annotated[
            Union[EnhancedFinancialCommitment, EnhancedVolumeCommitment],
            Field(discriminator="commitment_type"),
        ]
    ] = Field()
    iot_rates: list[
        Annotated[
            Union[EnhancedTieredService, EnhancedBalancedService],
            Field(discriminator="model_type"),
        ]
    ] = Field()
    tap_rates: list[EnhancedTapRateService] = Field()


class Enhanced_AA12_DealData(AA12_DealData):
    client_to_partner: Optional[EnhancedDirectionalData] = Field(default=None)
    partner_to_client: Optional[EnhancedDirectionalData] = Field(default=None)

    inbound: Optional[EnhancedDirectionalData] = Field(default=None)
    outbound: Optional[EnhancedDirectionalData] = Field(default=None)
