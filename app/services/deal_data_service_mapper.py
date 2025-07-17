from typing import Optional, Tuple
from uuid import UUID

import polars as pl
from polars import DataFrame

from app.pydantic_models.deal_data import AA12_DealData, DestinationType, IoTService

# BIF keys

SERVICE_TYPE_KEY = "service_type"

HOME_COUNTRY_KEY = "home_country"
VISITING_COUNTRY_KEY = "destination_country"
CALLED_COUNTRY_KEY = "called_country_code"

HOME_PMN_KEY = "home_pmn_code"
VISITING_PMN_KEY = "visitor_pmn_code"


class DealDataServiceMapper:
    """
    A service for mapping BIF DataFrame rows to deal data service UUIDs.
    """

    def __init__(self, deal_data: AA12_DealData):
        """
        Initialize the mapper with deal data.

        Args:
            deal_data: The AA12 deal data containing IoT rates, commitments, and tap rates
        """
        self.deal_data = deal_data

    @staticmethod
    def derive_destination_type(hcc: str, vcc: str, ccc: str) -> DestinationType:
        """
        Derives the destination type (HOME / LOCAL / INTERNATIONAL) based on HCC, VCC, and CCC.

        HCC: Home Country Code
        VCC: Visiting Country Code
        CCC: Called Country Code
        """
        for cc in (hcc, vcc, ccc):
            if len(cc) != 3:
                raise ValueError(f"Country code '{cc}' is not 3 characters long.")
            elif not cc.isupper():
                raise ValueError(f"Country code '{cc}' is not uppercase.")
            elif not cc.isalpha():
                raise ValueError(
                    f"Country code '{cc}' contains non-alphabetic characters."
                )

        if hcc == ccc:
            return DestinationType.home
        elif vcc == ccc:
            return DestinationType.local
        else:
            return DestinationType.international

    def map_all_bifs(self, bif: DataFrame) -> DataFrame:
        """Maps all BIF DataFrame rows to deal data and adds UUID columns."""
        # Create lists to store UUIDs for each row
        service_uuids = []
        commitment_uuids = []
        service_rate_uuids = []
        tap_uuids = []

        for row in bif.iter_rows(named=True):
            service_uuid, commitment_uuid, service_rate_uuid, tap_uuid = (
                self.map_bif_to_service_uuids(row)
            )
            service_uuids.append(service_uuid)
            commitment_uuids.append(commitment_uuid)
            service_rate_uuids.append(service_rate_uuid)
            tap_uuids.append(tap_uuid)


        # Add the UUID columns to the DataFrame
        return bif.with_columns(
            [
                pl.Series("service_uuid", service_uuids),
                pl.Series("commitment_uuid", commitment_uuids),
                pl.Series("service_rate_uuid", service_rate_uuids),
                pl.Series("tap_uuid", tap_uuids),
            ]
        )

    def _find_service_uuid(
        self,
        bif_row: dict,
        service: IoTService,
        destination_type: Optional[DestinationType] = None,
    ) -> Optional[UUID]:
        """Find matching service UUID from IoT rates."""

        if self.deal_data.inbound:
            for iot_rate in (
                self.deal_data.inbound.iot_rates
            ):  # todo: check if this is correct or if it is outbound
                if iot_rate.service == service:
                    if (
                        bif_row[HOME_PMN_KEY] in iot_rate.serving_party
                        and bif_row[VISITING_PMN_KEY] in iot_rate.served_party
                    ):
                        # Check destination type if required
                        if destination_type is None:
                            return iot_rate.uuid
                        elif iot_rate.destination in (
                            destination_type,
                            DestinationType.all,
                        ):
                            return iot_rate.uuid
        return None

    def _find_commitment_uuid(
        self,
        bif_row: dict,
        service: IoTService,
        destination_type: Optional[DestinationType] = None,
    ) -> Tuple[Optional[UUID], Optional[UUID]]:
        """Find matching commitment UUID and service rate UUID."""

        if self.deal_data.inbound:
            for commitment in (
                self.deal_data.inbound.commitments
            ):  # todo: check if this is correct or if it is outbound
                if (
                    bif_row[HOME_PMN_KEY] in commitment.serving_party
                    and bif_row[VISITING_PMN_KEY] in commitment.served_party
                ):
                    # Check destination type if required
                    if destination_type is not None:
                        if commitment.destination not in (
                            destination_type,
                            DestinationType.all,
                        ):
                            continue

                    # Find matching service rate
                    for service_rate in commitment.service_rates:
                        if service_rate.service == service:
                            return commitment.uuid, service_rate.uuid
        return (None, None)

    def _find_tap_uuid(
        self,
        bif_row: dict,
        service: IoTService,
        destination_type: Optional[DestinationType] = None,
    ) -> Optional[UUID]:
        """Find matching tap UUID."""

        if self.deal_data.inbound:
            for tap_rate in (
                self.deal_data.inbound.tap_rates
            ):  # todo: check if this is correct or if it is outbound
                if tap_rate.service == service:
                    if (
                        bif_row[HOME_PMN_KEY] in tap_rate.serving_party
                        and bif_row[VISITING_PMN_KEY] in tap_rate.served_party
                    ):
                        # Check destination type if required
                        if destination_type is None:
                            return tap_rate.uuid
                        elif tap_rate.destination in (
                            destination_type,
                            DestinationType.all,
                        ):
                            return tap_rate.uuid
        return None

    def _map_bif_to_service_uuids_with_destination(
        self, bif_row: dict, service: IoTService
    ) -> Tuple[Optional[UUID], Optional[UUID], Optional[UUID], Optional[UUID]]:
        """
        Maps a BIF DataFrame row to service, commitment and tap UUIDs for services that use destination type.

        Used for: SMS, Voice MO, Voice MT, VoLTE
        """

        destination_type = self.derive_destination_type(
            hcc=bif_row[HOME_COUNTRY_KEY],
            vcc=bif_row[VISITING_COUNTRY_KEY],
            ccc=bif_row[CALLED_COUNTRY_KEY],
        )

        service_uuid = self._find_service_uuid(bif_row, service, destination_type)
        commitment_uuid, service_rate_uuid = self._find_commitment_uuid(
            bif_row, service, destination_type
        )
        tap_uuid = self._find_tap_uuid(bif_row, service, destination_type)

        return (service_uuid, commitment_uuid, service_rate_uuid, tap_uuid)

    def _map_bif_to_service_uuids_without_destination(
        self, bif_row: dict, service: IoTService
    ) -> Tuple[Optional[UUID], Optional[UUID], Optional[UUID], Optional[UUID]]:
        """
        Maps a BIF DataFrame row to service, commitment, service rate and tap UUIDs for services that don't use destination type.

        Used for: Data, Voice MT
        """

        service_uuid = self._find_service_uuid(bif_row, service)
        commitment_uuid, service_rate_uuid = self._find_commitment_uuid(
            bif_row, service
        )
        tap_uuid = self._find_tap_uuid(bif_row, service)

        return (service_uuid, commitment_uuid, service_rate_uuid, tap_uuid)

    def _map_bif_to_service_uuids_sms(
        self, bif_row: dict
    ) -> Tuple[Optional[UUID], Optional[UUID], Optional[UUID], Optional[UUID]]:
        """
        Maps a BIF DataFrame row for SMS service to service, commitment, service rate and tap UUIDs.

        If no matching service, commitment or tap is found, returns None for each.
        """

        return self._map_bif_to_service_uuids_with_destination(bif_row, IoTService.sms)

    def _map_bif_to_service_uuids_voicemo(
        self,
        bif_row: dict,
    ) -> Tuple[Optional[UUID], Optional[UUID], Optional[UUID], Optional[UUID]]:
        """
        Maps a BIF DataFrame row for Voice MO service to service, commitment, service rate and tap UUIDs.

        If no matching service, commitment or tap is found, returns None for each.
        """

        return self._map_bif_to_service_uuids_with_destination(
            bif_row, IoTService.voice_mo
        )

    def _map_bif_to_service_uuids_data(
        self,
        bif_row: dict,
    ) -> Tuple[Optional[UUID], Optional[UUID], Optional[UUID], Optional[UUID]]:
        """
        Maps a BIF DataFrame row for Data service to service, commitment, service rate and tap UUIDs.

        If no matching service, commitment or tap is found, returns None for each.
        """

        return self._map_bif_to_service_uuids_without_destination(
            bif_row, IoTService.data
        )

    def _map_bif_to_service_uuids_voicemt(
        self,
        bif_row: dict,
    ) -> Tuple[Optional[UUID], Optional[UUID], Optional[UUID], Optional[UUID]]:
        """
        Maps a BIF DataFrame row for Voice MT service to service, commitment, service rate and tap UUIDs.

        If no matching service, commitment or tap is found, returns None for each.
        """

        return self._map_bif_to_service_uuids_without_destination(
            bif_row, IoTService.voice_mt
        )

    def _map_bif_to_service_uuids_volte(
        self,
        bif_row: dict,
    ) -> Tuple[Optional[UUID], Optional[UUID], Optional[UUID], Optional[UUID]]:
        """
        Maps a BIF DataFrame row for VoLTE service to service, commitment, service rate and tap UUIDs.

        If no matching service, commitment or tap is found, returns None for each.

        When no VoLTE rates are defined, we fall back to Data rates.
        """

        # Try VoLTE first
        service_uuid, commitment_uuid, service_rate_uuid, tap_uuid = (
            self._map_bif_to_service_uuids_with_destination(bif_row, IoTService.vo_lte)
        )

        # If no VoLTE rates found, fall back to Data rates
        if (
            service_uuid is None
            or commitment_uuid is None
            or service_rate_uuid is None
            or tap_uuid is None
        ):
            (
                data_service_uuid,
                data_commitment_uuid,
                data_service_rate_uuid,
                data_tap_uuid,
            ) = self._map_bif_to_service_uuids_without_destination(
                bif_row, IoTService.data
            )

            return (
                service_uuid or data_service_uuid,
                commitment_uuid or data_commitment_uuid,
                service_rate_uuid or data_service_rate_uuid,
                tap_uuid or data_tap_uuid,
            )

        return (service_uuid, commitment_uuid, service_rate_uuid, tap_uuid)

    def map_bif_to_service_uuids(
        self, bif_row: dict
    ) -> Tuple[Optional[UUID], Optional[UUID], Optional[UUID], Optional[UUID]]:
        """
        Maps a BIF DataFrame to a tuple of service, committment, service rate and tap UUIDs based on a relevant deal data.

        When Service UUID is none, we fall back to AA14 rates.
        """

        match bif_row[SERVICE_TYPE_KEY]:
            case IoTService.sms:
                return self._map_bif_to_service_uuids_sms(bif_row)
            case IoTService.data:
                return self._map_bif_to_service_uuids_data(bif_row)
            case IoTService.voice_mo:
                return self._map_bif_to_service_uuids_voicemo(bif_row)
            case IoTService.voice_mt:
                return self._map_bif_to_service_uuids_voicemt(bif_row)
            case IoTService.vo_lte:
                return self._map_bif_to_service_uuids_volte(bif_row)
            case _:
                return (None, None, None, None)
