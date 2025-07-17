from dataclasses import dataclass, field
from typing import List


@dataclass
class ServiceMapping:
    """Complete mapping for a service including all column names"""

    service_name: str
    bolt_service_name: str
    charge_incl_tax_col: str
    charge_excl_tax_col: str
    volume_charged_col: str
    volume_chargeable_col: str
    pmn_code_col: str
    called_country_iso_code: str
    volume_type: str = "duration"  # For documentation purposes
    operator_name_col: str = "hplmn_operator_name"
    date_col: str = "callmonth"
    currency_code_col: str = "currency"
    imsi_col: str = "no_imsi"
    home_country_col: str = "country"
    destination_country_col: str = "called_country_name"
    pct_of_total_charge_col: str = "_of_total_charge"  #


@dataclass
class FTPConfig:
    """Base FTP configuration"""

    org_name: str = "NGC"
    host: str = "127.0.0.1"
    port: int = 21
    user: str = "one"
    password: str = "1234"
    remote_dir: str = "/ftp/one"
    file_pattern_match: str = r"^(.*_MFS_.*)$"
    poll_interval: int = 10
    operator_location_in_file_name: int = (
        0  # this is in array pointer number so 0 = 1st
    )
    skip_rows: int = 1
    # New fields for home/visiting and PMN code
    home_file_pattern: str = r".*_PAY_.*"  # Regex to identify 'home' files
    visiting_file_pattern: str = r".*_REC_.*"  # Regex to identify 'visiting' files
    pmn_code_location_in_file_name: int = (
        0  # Index of the PMN code in the filename split by '_'
    )
    pmn_code_length: int = 5  # Length of the PMN code (e.g., "WSMDP" is 5 characters)

    service_mappings: List[ServiceMapping] = field(
        default_factory=lambda: [
            ServiceMapping(
                service_name="MOC_telephony",
                bolt_service_name="call_type",
                charge_incl_tax_col="moc_telephony__charge_incl_tax",
                charge_excl_tax_col="moc_telephony__charge_excl_tax",
                volume_charged_col="moc_telephony__durationmin_charged",
                volume_chargeable_col="moc_telephony__durationmin_chargable",
                called_country_iso_code="called_country_iso_code",
                volume_type="duration",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="MOC_fax",
                bolt_service_name="call_type",
                charge_incl_tax_col="moc_fax__charge_incl_tax",
                charge_excl_tax_col="moc_fax__charge_excl_tax",
                volume_charged_col="moc_fax__durationmin",
                volume_chargeable_col="moc_fax__durationmin",
                called_country_iso_code="called_country_iso_code",
                volume_type="duration",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="MOC_data",
                bolt_service_name="call_type",
                charge_incl_tax_col="moc_data__charge_incl_tax",
                charge_excl_tax_col="moc_data__charge_excl_tax",
                volume_charged_col="moc_data__durationmin",
                volume_chargeable_col="moc_data__durationmin",
                called_country_iso_code="called_country_iso_code",
                volume_type="duration",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="MOC_SMS",
                bolt_service_name="call_type",
                charge_incl_tax_col="moc_sms__charge_incl_tax",
                charge_excl_tax_col="moc_sms__charge_excl_tax",
                volume_charged_col="moc_sms__no_records",
                volume_chargeable_col="moc_sms__no_records",
                called_country_iso_code="called_country_iso_code",
                volume_type="records",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="MOC_other_services",
                bolt_service_name="call_type",

                charge_incl_tax_col="moc_other_services__charge_incl_tax",
                charge_excl_tax_col="moc_other_services__charge_excl_tax",
                volume_charged_col="moc_other_services__durationmin",
                volume_chargeable_col="moc_other_services__durationmin",
                called_country_iso_code="called_country_iso_code",
                volume_type="duration",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="MTC_telephony",
                bolt_service_name="call_type",
                charge_incl_tax_col="mtc_telephony__charge_incl_tax",
                charge_excl_tax_col="mtc_telephony__charge_excl_tax",
                volume_charged_col="mtc_telephony__durationmin_charged",
                volume_chargeable_col="mtc_telephony__durationmin_chargable",
                volume_type="duration",
                called_country_iso_code="called_country_iso_code",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="MTC_fax",
                bolt_service_name="call_type",
                charge_incl_tax_col="mtc_fax__charge_incl_tax",
                charge_excl_tax_col="mtc_fax__charge_excl_tax",
                volume_charged_col="mtc_fax__durationmin",
                volume_chargeable_col="mtc_fax__durationmin",
                called_country_iso_code="called_country_iso_code",
                volume_type="duration",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="MTC_data",
                bolt_service_name="call_type",
                charge_incl_tax_col="mtc_data__charge_incl_tax",
                charge_excl_tax_col="mtc_data__charge_excl_tax",
                volume_charged_col="mtc_data__durationmin",
                volume_chargeable_col="mtc_data__durationmin",
                called_country_iso_code="called_country_iso_code",
                volume_type="duration",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="MTC_SMS",
                bolt_service_name="call_type",
                charge_incl_tax_col="mtc_sms__charge_incl_tax",
                charge_excl_tax_col="mtc_sms__charge_excl_tax",
                volume_charged_col="mtc_sms__no_records",
                volume_chargeable_col="mtc_sms__no_records",
                called_country_iso_code="called_country_iso_code",
                volume_type="records",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="MTC_other_services",
                bolt_service_name="call_type",
                charge_incl_tax_col="mtc_other_services__charge_incl_tax",
                charge_excl_tax_col="mtc_other_services__charge_excl_tax",
                volume_charged_col="mtc_other_services__durationmin",
                volume_chargeable_col="mtc_other_services__durationmin",
                called_country_iso_code="called_country_iso_code",
                volume_type="duration",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="GPRS",
                bolt_service_name="call_type",
                charge_incl_tax_col="gprs__charge_incl_tax",
                charge_excl_tax_col="gprs__charge_excl_tax",
                volume_charged_col="gprs__volumekb_charged",
                volume_chargeable_col="gprs__volumekb_chargable",
                called_country_iso_code="called_country_iso_code",
                volume_type="volume",
                pmn_code_col="tadig",
            ),
            ServiceMapping(
                service_name="Other_calltypes",
                bolt_service_name="call_type",
                charge_incl_tax_col="other_calltypes__charge_incl_tax",
                charge_excl_tax_col="other_calltypes__charge_excl_tax",
                volume_charged_col="other_calltypes__durationmin",
                volume_chargeable_col="other_calltypes__durationmin",
                called_country_iso_code="called_country_iso_code",
                volume_type="duration",
                pmn_code_col="tadig",
            ),
        ]
    )


@dataclass
class PrefectConfig:
    server_url: str = "http://localhost:4200/api"
    deployment_name: str = "process-csv-flow/deployment"
    use_direct_execution: bool = True
