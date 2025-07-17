from datetime import datetime, timedelta, timezone
from enum import Enum, EnumMeta, StrEnum
from typing import Annotated, Literal, Optional, Union
from uuid import UUID, uuid4

from pydantic import AwareDatetime, Field, field_validator, model_validator
from typing_extensions import Self

from .utils import BaseSchema, SystemCustomType

# from typing import Decimal TODO

"""
How currency, rate, rate_unit, rate_type, charge_unit, charge_type, threshold, threshold_type are related:
<CURRENCY> <RATE> per <RATE_UNIT> <RATE_TYPE>, charged every <CHARGE_UNIT> <CHARGE_TYPE>
     £      0.10  per     10          MB     , charged every       1              GB
"""


class MetaEnum(EnumMeta):
    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class IoTService(StrEnum, metaclass=MetaEnum):
    voice_mo = "voice_mo"
    sms = "sms"
    data = "data"
    voice_mt = "voice_mt"
    vo_lte = "volte"


class VoiceRateType(StrEnum, metaclass=MetaEnum):
    seconds = "seconds"
    minutes = "minutes"


class SmsRateType(StrEnum, metaclass=MetaEnum):
    sms = "sms"


class DataRateType(StrEnum, metaclass=MetaEnum):
    kb = "KB"
    mb = "MB"
    gb = "GB"
    tb = "TB"


class Directions(StrEnum, metaclass=MetaEnum):
    unilateral = "unilateral"
    bilateral = "bilateral"


class DestinationType(StrEnum, metaclass=MetaEnum):
    home = "home"
    local = "local"
    international = "international"
    all = "all"


class Tier(BaseSchema):
    rate: float = Field(examples=[0.1])
    rate_unit: float = Field(examples=[1])
    rate_type: Annotated[str, Union[VoiceRateType, SmsRateType, DataRateType]] = Field()
    charge_unit: float = Field(examples=[10])
    charge_type: Annotated[str, Union[VoiceRateType, SmsRateType, DataRateType]] = (
        Field()
    )
    threshold: Optional[int] = Field(default=None, examples=[1, 60, None])
    threshold_type: Optional[
        Annotated[str, Union[VoiceRateType, SmsRateType, DataRateType]]
    ] = Field(default=None, examples=["MB", "GB", None])

    @model_validator(mode="after")
    def check_thresholds(self) -> Self:
        if (self.threshold is None) != (self.threshold_type is None):
            raise ValueError(
                "Both threshold and threshold_type must be either defined or not defined"
            )
        if self.threshold is not None and self.threshold < 0:
            raise ValueError("Threshold must be greater than 0")

        return self

    # def get_effective_rate(self) -> float:
    #     """
    #     Calculate the effective rate normalized by rate type

    #     VoiceRateType: per minute
    #     SmsRateType: per SMS
    #     DataRateType: per MB
    #     """
    #     base_rate = self.rate / self.rate_unit

    #     # Voice rates
    #     if self.rate_type in set(item.value for item in VoiceRateType):
    #         if self.rate_type == VoiceRateType.seconds:
    #             return base_rate * 60  # Convert to per minute
    #         elif self.rate_type == VoiceRateType.minutes:
    #             return base_rate
    #         else:
    #             raise ValueError("Invalid Voice Rate Type")

    #     # Data rates
    #     elif self.rate_type in set(item.value for item in DataRateType):
    #         if self.rate_type == DataRateType.kb:
    #             return base_rate * 1024  # Convert to per MB
    #         elif self.rate_type == DataRateType.mb:
    #             return base_rate
    #         elif self.rate_type == DataRateType.gb:
    #             return base_rate / 1024  # Convert to per MB
    #         elif self.rate_type == DataRateType.tb:
    #             return base_rate / (1024**2)  # Convert to per MB
    #         else:
    #             raise ValueError("Invalid Data Rate Type")

    #     # SMS rates (already normalized)
    #     elif self.rate_type in set(item.value for item in SmsRateType):
    #         return base_rate

    #     raise ValueError(f"Invalid Rate Type: {self.rate_type}")


class _BaseService(BaseSchema):
    destination: Annotated[str, DestinationType] = Field(examples=[DestinationType.all])

    serving_party: list[str] = Field(examples=["PMN01"])
    served_party: list[str] = Field(examples=["PMN02"])

    @field_validator("serving_party", "served_party")
    @classmethod
    def check_valid_pmns(cls, v):
        for pmn in v:
            if len(pmn) != 5:
                raise ValueError("PMN must be 5 chars long")
            if not pmn.isalnum():
                raise ValueError("PMN must be alphanumeric")
            if not pmn[0:3].isalpha():
                raise ValueError("PMN must start with 3 letters for country code")

        if len(v) < 1:
            raise ValueError("Must have at least 1 PMN for serving/served party")

        return v


class TapRateService(_BaseService):
    uuid: UUID = Field(default_factory=uuid4, examples=[uuid4()])
    model_type: Literal["tap_rate"]
    service: Annotated[str, IoTService] = Field(examples=[IoTService.voice_mo])

    rate: float = Field(examples=[0.1])
    rate_unit: float = Field(examples=[1])
    rate_type: Annotated[str, Union[VoiceRateType, SmsRateType, DataRateType]] = Field()
    charge_unit: float = Field(examples=[10])
    charge_type: Annotated[str, Union[VoiceRateType, SmsRateType, DataRateType]] = (
        Field()
    )


class _BaseTieredService(_BaseService):
    """TieredService covers both Tiered and Flat Rate models:
    Flat Rate is just Tiered without any tiers - one Tier with TODO"""

    uuid: UUID = Field(default_factory=uuid4, examples=[uuid4()])
    service: Annotated[str, IoTService] = Field(examples=[IoTService.voice_mo])
    back_to_first: bool = Field(examples=[True, False])
    tiers: list[Tier] = Field()  # TODO: check types of tiered rates

    @field_validator("tiers")
    @classmethod
    def check_valid_tiers(cls, v: list[Tier]):
        if len(v) < 1:
            raise ValueError("Must have at least 1 tier")

        # Check that rates decrease and thresholds increase as tiers progress
        if len(v) > 1:
            for i in range(1, len(v)):
                # Check thresholds if present
                lower_threshold = v[i - 1].threshold
                upper_threshold = v[i].threshold

                if upper_threshold and lower_threshold:
                    if upper_threshold <= lower_threshold:
                        raise ValueError(
                            f"Tier {i + 1} threshold must be greater than tier {i} threshold"
                        )

        return v


class TieredService(_BaseTieredService):
    model_type: Literal["structured"]  # tiered or flat


class BalancedService(_BaseTieredService):
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


class ServiceRate(BaseSchema):
    uuid: UUID = Field(default_factory=uuid4, examples=[uuid4()])
    service: Annotated[str, IoTService] = Field(examples=[IoTService.voice_mo])
    rate: float = Field(examples=[0.1])
    rate_unit: float = Field(examples=[1])
    rate_type: Annotated[str, Union[VoiceRateType, SmsRateType, DataRateType]] = Field()


class FinancialCommitment(_BaseService):
    """Define an amount (e.g., 1000 -> £1,000), and rates for each service.
    The commitment is reached by a composite of all these service rates;
    e.g. £0.10 per 1 minute for voVoiceice, £0.01 per 1 SMS, £0.10 per 1.5 MB of Data.
    """

    uuid: UUID = Field(default_factory=uuid4, examples=[uuid4()])

    commitment_type: Literal["financial"]

    amount: float = Field(examples=[1, 60])
    service_rates: list[ServiceRate] = Field()


class VolumeCommitment(_BaseService):
    """Define a volume and volume type (e.g., (1000, minutes) -> 1,000 minutes),
    and a ServiceRate, which specifies a single service and the price paid per unit of that service
    """

    uuid: UUID = Field(default_factory=uuid4, examples=[uuid4()])

    commitment_type: Literal["volume"]

    service_rates: list[ServiceRate] = Field()
    volume: int = Field(examples=[1, 60])
    volume_type: Annotated[str, Union[VoiceRateType, SmsRateType, DataRateType]] = (
        Field()
    )

    def _values_in_same_enum(self, value1: str, value2: str) -> bool:
        """Determine which enum type a value belongs to"""

        enum_types = [VoiceRateType, SmsRateType, DataRateType]
        for enum_type in enum_types:
            if value1 in enum_type and value2 in enum_type:
                return True
        return False

    @model_validator(mode="after")
    def check_valid_volume_type(self) -> Self:
        if len(self.service_rates) != 1:
            raise ValueError("Volume commitment must have exactly one service")

        # Check that both fields belong to the same enum type
        if not self._values_in_same_enum(
            self.service_rates[0].rate_type, self.volume_type
        ):
            raise ValueError(
                "Volume type and rate type must belong to the same enum type"
            )

        return self


class DirectionalData(BaseSchema):
    currency_code: str = Field(examples=["GBP"])
    tax: bool = Field(examples=[True, False])
    commitments: list[
        Annotated[
            Union[FinancialCommitment, VolumeCommitment],
            Field(discriminator="commitment_type"),
        ]
    ] = Field()
    iot_rates: list[
        Annotated[
            Union[TieredService, BalancedService],
            Field(discriminator="model_type"),
        ]
    ] = Field()
    tap_rate_tax: Optional[bool] = Field(
        default=None
    )  # this is never returned as None as it will be set to tax value if unset
    tap_rate_currency_code: Optional[str] = Field(examples=["GBP"])
    tap_rates: list[TapRateService] = Field()

    @model_validator(mode="after")
    def set_default_tap_rate_tax(self) -> Self:
        if self.tap_rate_tax is None:
            self.tap_rate_tax = self.tax
        return self

    def get_committment_service_destinations(self) -> dict[str, list[str]]:
        """Get a dictionary of services for the committments and list of their destinations (Home / Local / International)"""

        service_destinations: dict[str, list[str]] = {}
        for commitment in self.commitments:
            for item in commitment.service_rates:
                if item.service in service_destinations:
                    service_destinations[item.service].append(commitment.destination)
                else:
                    service_destinations[item.service] = [commitment.destination]
        return service_destinations

    def get_iot_service_destinations(self) -> dict[str, list[str]]:
        """Get a dictionary of services for the iot rates and list of their destinations (Home / Local / International)"""

        service_destinations: dict[str, list[str]] = {}
        for iot_rate in self.iot_rates:
            if iot_rate.service in service_destinations:
                if iot_rate.destination == DestinationType.all:
                    service_destinations[iot_rate.service].extend(
                        ["home", "local", "international"]
                    )
                else:
                    service_destinations[iot_rate.service].append(iot_rate.destination)
            else:
                service_destinations[iot_rate.service] = [iot_rate.destination]
        return service_destinations

    def get_tap_service_destinations(self) -> dict[str, list[str]]:
        """Get a dictionary of services for the tap rates and list of their destinations (Home / Local / International)"""

        service_destinations: dict[str, list[str]] = {}
        for tap_rate in self.tap_rates:
            if tap_rate.service in service_destinations:
                service_destinations[tap_rate.service].append(tap_rate.destination)
            else:
                service_destinations[tap_rate.service] = [tap_rate.destination]
        return service_destinations

    @model_validator(mode="after")
    def check_iot_rates_service_destination(self) -> Self:
        """Check that each IoT rate has a unique service and destination"""

        iot_service_destinations: dict[str, list] = self.get_iot_service_destinations()

        for service, destinations in iot_service_destinations.items():
            if len(destinations) != len(set(destinations)):
                raise ValueError(
                    f"Service '{service}' cannot have multiple rates for the same destination"
                )
        return self

    @model_validator(mode="after")
    def check_tap_rates_curr_code(self) -> Self:
        """Check that the tap rate currency code is provided if tap rates are present"""

        if self.tap_rate_currency_code is None and len(self.tap_rates) > 0:
            raise ValueError(
                "Tap rate currency code must be provided if tap rates are present"
            )
        return self

    @model_validator(mode="after")
    def check_committments_service_destination(self) -> Self:
        """Check that you can only have one commitment per service and destination"""

        comittment_service_destinations = self.get_committment_service_destinations()
        iot_service_destinations = self.get_iot_service_destinations()

        for service, destinations in comittment_service_destinations.items():
            if len(destinations) != len(set(destinations)):
                raise ValueError(
                    f"Service '{service}' cannot have multiple commitments for the same destination"
                )
            if DestinationType.all in destinations and len(destinations) > 1:
                raise ValueError(
                    f"Service '{service}' cannot have 'all' and other destinations at the same time"
                )
            if service not in iot_service_destinations:
                raise ValueError(
                    f"Service '{service}' must have a corresponding IoT rate"
                )
            for _dest in destinations:
                if (
                    _dest not in iot_service_destinations[service]
                    and _dest != DestinationType.all
                ):
                    raise ValueError(
                        f"Committed Service '{service}' must have a corresponding IoT rate for destination '{_dest}'"
                    )
        return self

    @model_validator(mode="after")
    def check_tap_rates_service_destination(self) -> Self:
        """Check that each Tap rate has a unique service and destination"""

        tap_service_destinations = self.get_tap_service_destinations()
        iot_service_destinations = self.get_iot_service_destinations()

        for service, destinations in tap_service_destinations.items():
            if len(destinations) != len(set(destinations)):
                raise ValueError(
                    f"Service '{service}' cannot have multiple commitments for the same destination"
                )
            if DestinationType.all in destinations and len(destinations) > 1:
                raise ValueError(
                    f"Service '{service}' cannot have 'all' and other destinations at the same time"
                )
            if service not in iot_service_destinations:
                raise ValueError(
                    f"Service '{service}' must have a corresponding IoT rate"
                )
            for _dest in destinations:
                if (
                    _dest not in iot_service_destinations[service]
                    and _dest != DestinationType.all
                ):
                    raise ValueError(
                        f"Tap Rate Service '{service}' must have a corresponding IoT rate for destination '{_dest}'"
                    )
        return self


# TODO: validate that balanced deal is in both inbound & outbound


class ContractPeriod(BaseSchema):
    start_period: AwareDatetime = Field(examples=[datetime.now(tz=timezone.utc)])
    end_period: AwareDatetime = Field(
        examples=[datetime.now(tz=timezone.utc) + timedelta(days=365)]
    )

    @model_validator(mode="after")
    def check_valid_period(self) -> Self:
        if self.start_period > self.end_period:
            raise ValueError("Start period must be before end period")
        return self


class DirectionEnum(str, Enum):
    inbound = "inbound"
    outbound = "outbound"


class AddendumContent(BaseSchema):
    heading: str = Field(examples=["Addendum Heading", "Another Heading"])
    content: str = Field(examples=["Addendum content goes here.", "More content."])


class CreateAddendumRequest(AddendumContent):
    org_uuid: Optional[UUID] = Field(examples=[uuid4(), None], default=None)
    addendum_type: SystemCustomType = Field(examples=["custom", "system"])

    @model_validator(mode="after")
    def check_org_uuid(self) -> Self:
        if self.org_uuid is None and self.addendum_type == SystemCustomType.custom:
            raise ValueError("Organisation UUID is required for custom addendums")
        elif (
            self.org_uuid is not None and self.addendum_type == SystemCustomType.system
        ):
            raise ValueError("You cannot assign an org_uuid to a system addendum")
        return self


class AA12_DealData(BaseSchema):
    deal_type: str = "AA12"

    # Deal start + end date-times
    contract_period: ContractPeriod = Field()

    # UUID of the contract template used to create this deal
    # If the contract template is an off-platform deal, it will be set to "uploaded_contract"
    contract_template_uuid: Union[UUID, Literal["uploaded_contract"]] = Field(
        examples=[uuid4(), None]
    )
    client_uuid: UUID = Field(examples=[uuid4()])  # UUID of the ORG1 within the deal
    partner_uuid: UUID = Field(examples=[uuid4()])  # UUID of the ORG2 within the deal
    # Internal storage format, so we can set the inbound and outbound rates depending on the client or partner perspective
    client_to_partner: Optional[DirectionalData] = Field(default=None)
    partner_to_client: Optional[DirectionalData] = Field(default=None)

    # This is for the frontend rendering the deal data. If a deal is unilateral, it will be set to inbound or outbound. When bilateral, or saved in the DB, it will be set to None.
    direction: Optional[Annotated[str, DirectionEnum]] = Field(
        default=None,
        examples=[
            "inbound",
            "outbound",
        ],
    )
    # If the deal has both directions it is bilateral, otherwise it is unilateral
    laterality: Directions = Field(examples=[Directions.bilateral])

    # Frontend format (these will be computed to / from the internal format)
    inbound: Optional[DirectionalData] = Field(default=None)
    outbound: Optional[DirectionalData] = Field(default=None)

    # Extra contract terms
    addendums: list[CreateAddendumRequest] = Field()

    @model_validator(mode="after")
    def _validate_directions(self) -> Self:
        """Check that at least one direction is provided and that the laterality is valid given the directions"""

        if not any(
            [
                self.client_to_partner,
                self.partner_to_client,
                self.inbound,
                self.outbound,
            ]
        ):
            raise ValueError("At least one direction must be provided")
        if self.laterality == Directions.unilateral and (
            all([self.inbound, self.outbound])
            or all([self.client_to_partner, self.partner_to_client])
        ):
            raise ValueError("Unilateral agreements must have only one direction")
        if self.laterality == Directions.bilateral and not (
            all([self.inbound, self.outbound])
            or all([self.client_to_partner, self.partner_to_client])
        ):
            raise ValueError("Bilateral agreements must have both directions")
        return self

    @model_validator(mode="after")
    def _validate_balanced_deals(self) -> Self:
        if self.laterality == Directions.unilateral:
            for iot_rates in (
                self.inbound,
                self.outbound,
                self.client_to_partner,
                self.partner_to_client,
            ):
                if iot_rates:
                    for iot_rate in iot_rates.iot_rates:
                        if isinstance(iot_rate, BalancedService):
                            raise ValueError(
                                "Balanced deals are not allowed in unilateral agreements"
                            )
        else:
            if self.inbound and self.outbound:
                a, b = self.inbound, self.outbound
            elif self.client_to_partner and self.partner_to_client:
                a, b = self.client_to_partner, self.partner_to_client

            else:
                raise ValueError(
                    "Bilateral agreements must have both inbound and outbound directions"
                )

            balanced_rates_a: dict[str, dict] = {}
            balanced_rates_b: dict[str, dict] = {}

            for iot_rates, save_dict in ((a, balanced_rates_a), (b, balanced_rates_b)):
                for iot_rate in iot_rates.iot_rates:
                    if isinstance(iot_rate, BalancedService):
                        balanced_service_dest_key = str(
                            [iot_rate.service, iot_rate.destination]
                        )

                        save_dict[balanced_service_dest_key] = {
                            "service": iot_rate.service,
                            "destination": iot_rate.destination,
                            "balanced_rate": iot_rate.balanced_rate,
                            "balanced_rate_unit": iot_rate.balanced_rate_unit,
                            "balanced_rate_type": iot_rate.balanced_rate_type,
                        }

            for direction, other_direction in [
                (balanced_rates_a, balanced_rates_b),
                (balanced_rates_b, balanced_rates_a),
            ]:
                for key, value in direction.items():
                    if key in other_direction:
                        if value != other_direction[key]:
                            raise ValueError(
                                f"Balanced rates must be the same for both directions. Check the balanced rates for service {value['service']} for destination {value['destination']}"
                            )
                    else:
                        raise ValueError(
                            f"Balanced rate {value['service']} for destination {value['destination']} not found in both directions"
                        )

        return self

    def _from_client(self) -> Self:
        """Set the deal data from the client's perspective"""
        self.client_to_partner, self.partner_to_client = (self.outbound, self.inbound)
        self.inbound, self.outbound = None, None
        return self

    def _from_partner(self) -> Self:
        """Set the deal data from the partner's perspective"""
        self.client_to_partner, self.partner_to_client = (self.inbound, self.outbound)
        self.inbound, self.outbound = None, None
        return self

    def _for_client(self) -> Self:
        """Get the deal data formatted for the client's perspective"""
        self.inbound, self.outbound = (self.partner_to_client, self.client_to_partner)
        self.client_to_partner, self.partner_to_client = None, None
        return self

    def _for_partner(self) -> Self:
        """Get the deal data formatted for the partner's perspective"""
        self.inbound, self.outbound = (self.client_to_partner, self.partner_to_client)
        self.client_to_partner, self.partner_to_client = None, None
        return self

    def to_frontend_format(self, org_uuid: UUID) -> Self:
        """Get the deal data formatted for the frontend based on the user's org_uuid"""
        is_client = org_uuid == self.client_uuid
        frontend_format = self._for_client() if is_client else self._for_partner()
        if frontend_format.laterality == Directions.unilateral:
            if frontend_format.inbound:
                frontend_format.direction = DirectionEnum.inbound
            else:
                frontend_format.direction = DirectionEnum.outbound
        return frontend_format

    def from_frontend_format(self, org_uuid: UUID) -> Self:
        """Set the deal data from the frontend based on the user's org_uuid"""
        is_client = org_uuid == self.client_uuid
        self.direction = None
        return self._from_client() if is_client else self._from_partner()
