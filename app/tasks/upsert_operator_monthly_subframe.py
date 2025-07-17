from datetime import datetime
from uuid import UUID

import polars as pl
from polars import DataFrame
from prefect import task

from app.db.connection import get_session
from app.sqlalchemy_schemas import MonthlyTable

@task
def upsert_operator_monthly_subframe(bif: pl.DataFrame, file_uuid: UUID, contract_uuid: UUID) -> None:
    """
    Upserts operator subframe data into the MonthlyTable, handling date
    normalization and data type conversions, with an in-loop existence check.
    """
    session = get_session()

    with session as db:
        for row_dict in bif.to_dicts():
            try:
                # 1. Date Conversion and Normalization
                date_value_raw = row_dict.get('date')
                if date_value_raw is None:
                    print(f"Error: 'date' column is missing or None in row: {row_dict}. Skipping row.")
                    continue

                parsed_date = None
                if isinstance(date_value_raw, datetime):
                    parsed_date = date_value_raw
                else:
                    date_str = str(date_value_raw).strip()
                    try:
                        # Attempt to parse as full ISO format
                        parsed_date = datetime.fromisoformat(date_str)
                    except ValueError:
                        # If that fails, try parsing as YYYYMM (e.g., '202505')
                        if len(date_str) == 6 and date_str.isdigit():
                            try:
                                year = int(date_str[:4])
                                month = int(date_str[4:])
                                parsed_date = datetime(year, month, 1)  # Sub in first of the month
                            except ValueError:
                                pass

                if parsed_date is None:
                    print(
                        f"Error: 'date' value '{date_value_raw}' is not in a recognized datetime format. Skipping row.")
                    continue

                # Normalize to the first of the month, as per schema comment
                normalized_date = parsed_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

                # 2. Volume Conversion
                volume_value = row_dict.get('volume_charged')
                if volume_value is not None:
                    try:
                        volume_value = float(volume_value)
                    except (ValueError, TypeError):
                        print(f"Error: 'volume_charged' value '{volume_value}' cannot be converted to float. Setting to 0.0.")
                        volume_value = 0.0
                else:
                    volume_value = 0.0

                service_type_value = str(row_dict.get('service_type', "")) if row_dict.get('service_type') is not None else ""
                hpmn_value = str(row_dict.get('home_pmn_code', "")) if row_dict.get('home_pmn_code') is not None else ""
                vpmn_value = str(row_dict.get('visitor_pmn_code', "")) if row_dict.get('visitor_pmn_code') is not None else ""

                # UUID fields can be None, so check explicitly
                # Ensure conversion to UUID object if not None, otherwise keep None
                service_uuid_value = (UUID(str(row_dict['service_uuid'])) if row_dict.get('service_uuid') is not None else None)
                commitment_uuid_value = (UUID(str(row_dict['commitment_uuid'])) if row_dict.get('commitment_uuid') is not None else None)
                tap_uuid_value = (UUID(str(row_dict['tap_uuid'])) if row_dict.get('tap_uuid') is not None else None)
                contract_uuid = contract_uuid if contract_uuid is not None else None
                # Corrected exists check using SQLAlchemy 🎯
                # We assume 'date' (normalized) and 'file_uuid' uniquely identify a monthly record.
                existing_record = (
                    db.query(MonthlyTable)
                    .filter(
                        MonthlyTable.date == normalized_date,
                        MonthlyTable.file_uuid == file_uuid,
                        MonthlyTable.hpmn == hpmn_value,
                        MonthlyTable.vpmn == vpmn_value,
                        MonthlyTable.service_type == service_type_value
                    )
                    .first()
                )
                if existing_record is None:
                    # Record does not exist, so insert it
                    record = MonthlyTable(
                        file_uuid=file_uuid,
                        date=normalized_date,
                        volume=volume_value,
                        service_type=service_type_value,
                        contract_uuid=contract_uuid,
                        hpmn=hpmn_value,
                        vpmn=vpmn_value,
                        service_uuid=service_uuid_value,
                        commitment_uuid=commitment_uuid_value,
                        tap_uuid=tap_uuid_value,
                    )
                    db.add(record)
                    print(f"Added new record for date {normalized_date}")
                else:
                    existing_record.volume = volume_value
                    existing_record.service_type = service_type_value
                    existing_record.hpmn = hpmn_value
                    existing_record.vpmn = vpmn_value
                    existing_record.contract_uuid = contract_uuid,
                    existing_record.service_uuid = service_uuid_value
                    existing_record.commitment_uuid = commitment_uuid_value
                    existing_record.tap_uuid = tap_uuid_value
                    print(f"Updated existing record for date {normalized_date}")

                db.commit()
                print(f"Transaction committed for date {normalized_date}")

            except KeyError as e:
                print(f"Error: Missing expected column in DataFrame row: {e}. Skipping row.")
                db.rollback() # Rollback if an error occurs for this row
                continue
            except (ValueError, TypeError) as e:
                print(f"Error: Data conversion or UUID parsing issue for row: {e}. Skipping row.")
                db.rollback() # Rollback if an error occurs for this row
                continue
            except Exception as e:
                print(f"An unexpected error occurred for row: {e}. Skipping row.")
                db.rollback() # Rollback if an error occurs for this row
                continue

    session.close()