from typing import Dict, List, Optional

import polars as pl
from polars import DataFrame
from prefect import task

import country_converter as coco  # Import the library


@task
def transform_to_bolt_format(
        df: pl.DataFrame,
        service_mappings: List[Dict[str, str]] = None,
        pmn: Optional[str] = None,
        file_type: Optional[str] = None,
) -> Optional[DataFrame]:
    """Transform wide format telecom data to long format using provided mappings"""
    print("\n--- Transforming data to long format...")

    if service_mappings is None:
        print("⚠️ No service mappings provided")
        return None
    target_col = "call_type"
    df = df.with_columns(
        pl.when(
            pl.col(target_col)
            .str.to_lowercase()
            .str.replace_all(" ", "_")
            == "gprs"
        )
        .then(pl.lit("data"))
        .when(
            pl.col(target_col)
            .str.to_lowercase()
            .str.replace_all(" ", "_")
            .str.starts_with("sms_")
        )
        .then(pl.lit("sms"))
        .otherwise(
            pl.col(target_col)
            .str.to_lowercase()
            .str.replace_all(" ", "_")
        )
        .alias(target_col)
    )

    # Vectorize country conversion once before the loop
    if "country" in df.columns and "country_iso3" not in df.columns:
        iso3_list = coco.convert(df["country"].to_list(), to="ISO3")
        df = df.with_columns(
            pl.Series(name="country_iso3", values=iso3_list)
        )
    print(f"this is  the dataframe after the small mapping step: {df}")

    # Collect all service data frames
    service_dfs = []

    # Determine the file owner's PMN code (us)
    owner_pmn = pmn if pmn else None

    for mapping in service_mappings:
        service_name = mapping.get("service_name")
        charge_incl_tax_col = mapping.get("charge_incl_tax_col")
        charge_excl_tax_col = mapping.get("charge_excl_tax_col")
        volume_charged_col = mapping.get("volume_charged_col")
        volume_chargeable_col = mapping.get("volume_chargeable_col")
        pmn_code_col = mapping.get("pmn_code_col")
        date_col = mapping.get("date_col", "callmonth")
        currency_code_col = mapping.get("currency_code_col", "currency")
        called_country_code_col = mapping.get("called_country_iso_code")
        imsi_col = mapping.get("imsi_col", "no_imsi")
        home_country_col = mapping.get("home_country_col", "country")
        destination_country_col = mapping.get("called_country_iso_code")
        pct_of_total_charge_col = mapping.get("pct_of_total_charge_col", "of_total_charge")
        call_type_col = mapping.get("bolt_service_name")

        if charge_excl_tax_col not in df.columns:
            print(f"⚠️ Skipping {service_name}: column {charge_excl_tax_col} not found")
            continue

        home_pmn_expr = None
        visited_pmn_expr = None
        roaming_partner_pmn_col = None
        if pmn_code_col and pmn_code_col in df.columns:
            roaming_partner_pmn_col = pmn_code_col
        elif "tadig" in df.columns:
            roaming_partner_pmn_col = "tadig"
        elif "hplmn_operator_id" in df.columns:
            roaming_partner_pmn_col = "hplmn_operator_id"
        elif "vplmn_operator_id" in df.columns:
            roaming_partner_pmn_col = "vplmn_operator_id"

        if file_type == "home":
            home_pmn_expr = pl.lit(owner_pmn).alias("home_pmn_code")
            if roaming_partner_pmn_col:
                visited_pmn_expr = pl.col(roaming_partner_pmn_col).alias("visitor_pmn_code")
            else:
                visited_pmn_expr = pl.lit(None).alias("visitor_pmn_code")
        elif file_type == "visiting":
            visited_pmn_expr = pl.lit(owner_pmn).alias("visitor_pmn_code")
            if roaming_partner_pmn_col:
                home_pmn_expr = pl.col(roaming_partner_pmn_col).alias("home_pmn_code")
            else:
                home_pmn_expr = pl.lit(None).alias("home_pmn_code")
        else:
            home_pmn_expr = pl.lit(None).alias("home_pmn_code")
            visited_pmn_expr = pl.lit(None).alias("visitor_pmn_code")

        columns_to_select = [
            pl.col(date_col).alias("date") if date_col in df.columns else pl.lit(None).alias("date"),
            pl.col(currency_code_col).alias("currency_code") if currency_code_col in df.columns else pl.lit(None).alias(
                "currency_code"),
            pl.col("country_iso3").alias("home_country") if "country_iso3" in df.columns else pl.lit(None).alias(
                "home_country"),
            pl.col(destination_country_col).alias(
                "destination_country") if destination_country_col in df.columns else pl.lit(None).alias(
                "destination_country"),
            pl.col(called_country_code_col).alias(
                "called_country_code") if called_country_code_col in df.columns else pl.lit(None).alias(
                "called_country_code"),
            home_pmn_expr,
            visited_pmn_expr,
        ]

        if call_type_col and call_type_col in df.columns:
            columns_to_select.append(
                pl.col(call_type_col)
                .str.to_lowercase()
                .str.replace_all(" ", "_")
                .alias("service_type")
            )
        else:
            columns_to_select.append(pl.lit(service_name).alias("service_type"))

        if volume_charged_col and volume_charged_col in df.columns:
            columns_to_select.append(
                pl.col(volume_charged_col)
                .cast(pl.Float64, strict=False)
                .fill_null(0)
                .alias("volume_charged")
            )
        else:
            columns_to_select.append(pl.lit(0.0).alias("volume_charged"))

        if volume_chargeable_col and volume_chargeable_col in df.columns:
            columns_to_select.append(
                pl.col(volume_chargeable_col)
                .cast(pl.Float64, strict=False)
                .fill_null(0)
                .alias("volume_chargeable")
            )
        else:
            columns_to_select.append(pl.lit(0.0).alias("volume_chargeable"))

        columns_to_select.append(
            pl.col(imsi_col).cast(pl.Int32, strict=False).fill_null(0).alias("imsi_used")
            if imsi_col in df.columns else pl.lit(0).alias("imsi_used")
        )

        columns_to_select.append(
            pl.col(charge_excl_tax_col)
            .cast(pl.Float64, strict=False)
            .fill_null(0)
            .alias("charge_excluding_tax")
        )

        if charge_incl_tax_col in df.columns:
            columns_to_select.append(
                pl.col(charge_incl_tax_col)
                .cast(pl.Float64, strict=False)
                .fill_null(0)
                .alias("charge_including_tax")
            )
        else:
            columns_to_select.append(
                pl.col(charge_excl_tax_col)
                .cast(pl.Float64, strict=False)
                .fill_null(0)
                .alias("charge_including_tax")
            )

        if pct_of_total_charge_col and pct_of_total_charge_col in df.columns:
            columns_to_select.append(
                pl.col(pct_of_total_charge_col)
                .cast(pl.Float64, strict=False)
                .fill_null(0)
                .alias("pct_of_total_charge")
            )
        else:
            columns_to_select.append(pl.lit(0.0).alias("pct_of_total_charge"))

        service_df = df.select(columns_to_select)

        service_df = service_df.filter(
            (pl.col("charge_excluding_tax") != 0)
            | (pl.col("charge_including_tax") != 0)
        )
        if len(service_df) > 0:
            service_dfs.append(service_df)

    if not service_dfs:
        print("No service data frames to concatenate. Returning empty DataFrame.")
        return pl.DataFrame()

    long_df = pl.concat(service_dfs, how="vertical")
    return long_df
