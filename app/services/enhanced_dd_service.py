from collections import defaultdict
from typing import Any, Tuple
from uuid import UUID

from app.db.connection import get_session
from app.sqlalchemy_schemas.monthly import MonthlyTable


def get_monthly_records_by_contract_uuid(
    session, contract_uuid: UUID
) -> list[MonthlyTable]:
    """
    Retrieves monthly records filtered by contract UUID.
    """
    return (
        session.query(MonthlyTable)
        .filter(MonthlyTable.contract_uuid == contract_uuid)
        .all()
    )


def get_volumes_by_uuids(
    records: list[MonthlyTable],
) -> Tuple[dict[str, float], dict[str, float], dict[str, float], dict[str, float]]:
    volumes_by_service_uuid = defaultdict(float)
    volumes_by_committment_uuid = defaultdict(float)
    volumes_by_tap_uuid = defaultdict(float)

    aa14_volumes_by_service_type = defaultdict(float)

    for record in records:
        if record.service_uuid:
            volumes_by_service_uuid[record.service_uuid] += record.volume
        else:
            aa14_volumes_by_service_type[record.service_type] += record.volume
        if record.commitment_uuid:
            volumes_by_committment_uuid[record.commitment_uuid] += record.volume

        if record.tap_uuid:
            volumes_by_tap_uuid[record.tap_uuid] += record.volume

    return (
        volumes_by_service_uuid,
        volumes_by_committment_uuid,
        volumes_by_tap_uuid,
        aa14_volumes_by_service_type,
    )


def create_enhanced_dd(deal_data: Any, contract_uuid: UUID):
    session = get_session()
    _records = get_monthly_records_by_contract_uuid(session, contract_uuid)
    (
        volumes_by_service_uuid,
        volumes_by_committment_uuid,
        volumes_by_tap_uuid,
        aa14_volumes_by_service_type,
    ) = get_volumes_by_uuids(_records)
