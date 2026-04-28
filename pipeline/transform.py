from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.credibility import (
    ACCOUNT_STATUSES,
    ACCOUNT_TYPES,
    DIGITAL_CHANNELS,
    GENDERS,
    KYC_STATUSES,
    PRODUCT_TIERS,
    SA_PROVINCES,
    TRANSACTION_CHANNELS,
    TRANSACTION_TYPES,
    domain_drift_settings,
    domain_values,
    invalid_domain,
    is_blank,
    load_credibility_rules,
)
from pipeline.common import (
    add_missing_columns,
    currency_variant_flag,
    date_variant_flag,
    load_config,
    load_dq_rules,
    metric_int,
    normalise_currency,
    output_path,
    parse_date,
    parse_timestamp,
    read_delta,
    spark_session,
    write_delta,
)
from pipeline.domain_drift import (
    approx_distinct_clean_unknown_values,
    approx_distinct_clean_values,
    approx_distinct_unknown_values,
    approx_distinct_values,
    light_domain_drift,
)
from pipeline.metrics import METRICS, add_issue, set_raw_profile_section
from pipeline.readiness import (
    coordinate_latitude,
    coordinate_longitude,
    currency_conversion_ready,
    currency_conversion_required,
    currency_iso_candidate,
    currency_normalized,
    currency_numeric_candidate,
    currency_readiness_summary,
    currency_unknown,
    geo_anomaly_flags,
    geo_invalid_world_bounds,
    geo_outside_sa_bounds,
    geo_parse_failed,
    geo_possible_swap,
    geo_province_mismatch,
    geo_quality_summary,
    geo_sa_sign_mismatch,
    geo_zero_zero,
)


DEFAULT_ACTIONS = {
    "DUPLICATE_DEDUPED": "DEDUPLICATED_KEEP_FIRST",
    "ORPHANED_ACCOUNT": "QUARANTINED",
    "TYPE_MISMATCH": "CAST_TO_DECIMAL",
    "DATE_FORMAT": "NORMALISED_DATE",
    "CURRENCY_VARIANT": "NORMALISED_CURRENCY",
    "NULL_REQUIRED": "EXCLUDED_NULL_PK",
}


def _action(issue_type, rules):
    rule = (rules.get("rules") or {}).get(issue_type, {})
    return rule.get("handling_action", DEFAULT_ACTIONS[issue_type])


def _dedupe(df, key, order_cols):
    window = Window.partitionBy(key).orderBy(*[F.col(c).asc_nulls_last() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def _count_if(predicate):
    return F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).cast("long")


def _aggregate_metrics(df, **expressions):
    row = df.agg(*[expr.alias(name) for name, expr in expressions.items()]).first()
    result = {}
    for name in expressions:
        value = row[name]
        if isinstance(value, (list, tuple)):
            result[name] = [float(item) if item is not None else None for item in value]
        else:
            result[name] = int(value or 0)
    return result


def _metric(count, total):
    count = int(count or 0)
    total = int(total or 0)
    return {
        "count": count,
        "pct": round(count * 100.0 / total, 2) if total else 0.0,
    }


def _dedupe_transactions_grouped(transactions):
    order_key = F.struct(
        F.col("transaction_timestamp").isNull().cast("int").alias("timestamp_is_null"),
        F.col("transaction_timestamp").alias("transaction_timestamp"),
        F.col("raw_json").alias("raw_json"),
    )
    row_value = F.struct(*[F.col(column).alias(column) for column in transactions.columns])
    return transactions.groupBy("transaction_id").agg(
        F.count(F.lit(1)).cast("long").alias("_group_count"),
        F.min_by(row_value, order_key).alias("_picked"),
    )


def _quarantined_duplicates(transactions, duplicate_ids):
    duplicate_window = Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_timestamp").asc_nulls_last(),
        F.col("raw_json"),
    )
    return (
        transactions.join(F.broadcast(duplicate_ids), "transaction_id", "inner")
        .withColumn("_rn", F.row_number().over(duplicate_window))
        .where(F.col("_rn") > 1)
        .drop("_rn")
    )


def _with_transaction_readiness_columns(df):
    return (
        df.withColumn("currency_raw", F.col("currency"))
        .withColumn("currency_normalized", currency_normalized(F.col("currency")))
        .withColumn("currency_iso_candidate", currency_iso_candidate(F.col("currency")))
        .withColumn("currency_numeric_candidate", currency_numeric_candidate(F.col("currency")))
        .withColumn("currency_conversion_required", currency_conversion_required(F.col("currency")))
        .withColumn("currency_conversion_ready", currency_conversion_ready(F.col("currency")))
        .withColumn("geo_coordinate_raw", F.col("location.coordinates"))
        .withColumn("geo_latitude", coordinate_latitude(F.col("location.coordinates")))
        .withColumn("geo_longitude", coordinate_longitude(F.col("location.coordinates")))
        .withColumn("geo_anomaly_flags", geo_anomaly_flags(F.col("geo_coordinate_raw"), F.col("province"), F.col("geo_latitude"), F.col("geo_longitude")))
    )


def _select_transactions_silver(df, include_provenance=False):
    if include_provenance:
        df = _with_transaction_readiness_columns(df)
    columns = [
        "transaction_id",
        "account_id",
        F.col("transaction_date_parsed").alias("transaction_date"),
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        "merchant_subcategory",
        F.col("amount_decimal").alias("amount"),
        F.col("currency_clean").alias("currency"),
    ]
    if include_provenance:
        columns.extend(
            [
                "currency_raw",
                "currency_normalized",
                "currency_iso_candidate",
                "currency_numeric_candidate",
                "currency_conversion_required",
                "currency_conversion_ready",
            ]
        )
    columns.extend(["channel", "province"])
    if include_provenance:
        columns.extend(["geo_coordinate_raw", "geo_latitude", "geo_longitude", "geo_anomaly_flags"])
    columns.extend(["dq_flag", "ingestion_timestamp"])
    return df.select(*columns)


def _set_light_raw_profile(
    source_transactions,
    source_accounts,
    source_customers,
    transaction_metrics,
    duplicate_count,
    deduped_metrics,
    account_metrics,
    customer_metrics,
    accounts_output_count,
    profile_settings=None,
):
    profile_settings = profile_settings or {}
    set_raw_profile_section(
        "tables",
        {
            "transactions": {
                "rows": source_transactions,
                "duplicate_transaction_surplus": _metric(duplicate_count, source_transactions),
                "required_nulls": {
                    "transaction_id": _metric(transaction_metrics["null_transaction_id"], source_transactions),
                    "account_id": _metric(transaction_metrics["null_transaction_account_id"], source_transactions),
                    "transaction_date": _metric(transaction_metrics["null_transaction_date"], source_transactions),
                    "transaction_time": _metric(transaction_metrics["null_transaction_time"], source_transactions),
                    "amount": _metric(transaction_metrics["null_transaction_amount"], source_transactions),
                },
                "amount_quality": {
                    "parse_failed": _metric(transaction_metrics["amount_cast_failed_count"], source_transactions),
                    "type_mismatch_or_parse_failed": _metric(transaction_metrics["type_count"], source_transactions),
                    "negative": _metric(transaction_metrics["negative_amount_count"], source_transactions),
                    "zero": _metric(transaction_metrics["zero_amount_count"], source_transactions),
                    "above_configured_extreme_threshold": _metric(transaction_metrics["extreme_amount_count"], source_transactions),
                    "quantiles": transaction_metrics.get("amount_quantiles", []),
                },
                "domain_quality": {
                    "invalid_transaction_type": _metric(transaction_metrics["invalid_transaction_type_count"], source_transactions),
                    "invalid_channel": _metric(transaction_metrics["invalid_channel_count"], source_transactions),
                    "currency_variant": _metric(transaction_metrics["currency_count"], source_transactions),
                    "invalid_currency": _metric(transaction_metrics["invalid_currency_count"], source_transactions),
                    "invalid_province": _metric(transaction_metrics["invalid_transaction_province_count"], source_transactions),
                },
                "temporal_quality": {
                    "date_variant": _metric(transaction_metrics["date_transaction_count"], source_transactions),
                    "date_parse_failed": _metric(transaction_metrics["transaction_date_parse_failed_count"], source_transactions),
                },
            },
            "accounts": {
                "rows": source_accounts,
                "required_nulls": {
                    "account_id": _metric(account_metrics["null_account_count"], source_accounts),
                    "customer_ref": _metric(account_metrics["null_customer_ref_count"], source_accounts),
                },
                "domain_quality": {
                    "invalid_account_type": _metric(account_metrics["invalid_account_type_count"], source_accounts),
                    "invalid_account_status": _metric(account_metrics["invalid_account_status_count"], source_accounts),
                    "invalid_product_tier": _metric(account_metrics["invalid_product_tier_count"], source_accounts),
                    "invalid_digital_channel": _metric(account_metrics["invalid_digital_channel_count"], source_accounts),
                },
                "temporal_quality": {
                    "date_variant": _metric(account_metrics["date_account_count"], source_accounts),
                    "open_date_parse_failed": _metric(account_metrics["open_date_parse_failed_count"], source_accounts),
                    "last_activity_before_open": _metric(account_metrics["last_activity_before_open_count"], source_accounts),
                },
                "numeric_quality": {
                    "negative_current_balance": _metric(account_metrics["negative_current_balance_count"], source_accounts),
                    "negative_credit_limit": _metric(account_metrics["negative_credit_limit_count"], source_accounts),
                    "current_balance_quantiles": account_metrics.get("current_balance_quantiles", []),
                },
            },
            "customers": {
                "rows": source_customers,
                "required_nulls": {
                    "customer_id": _metric(customer_metrics["null_customer_count"], source_customers),
                },
                "domain_quality": {
                    "invalid_gender": _metric(customer_metrics["invalid_gender_count"], source_customers),
                    "invalid_province": _metric(customer_metrics["invalid_customer_province_count"], source_customers),
                    "invalid_kyc_status": _metric(customer_metrics["invalid_kyc_status_count"], source_customers),
                },
                "temporal_quality": {
                    "date_variant": _metric(customer_metrics["date_customer_count"], source_customers),
                    "dob_parse_failed": _metric(customer_metrics["dob_parse_failed_count"], source_customers),
                },
                "numeric_quality": {
                    "risk_score_outside_range": _metric(customer_metrics["risk_score_outside_range_count"], source_customers),
                    "risk_score_quantiles": customer_metrics.get("risk_score_quantiles", []),
                },
            },
        },
    )
    set_raw_profile_section(
        "cross_table",
        {
            "referential_checks_enabled": True,
            "accounts_with_unknown_customer_ref": _metric(
                max(source_accounts - account_metrics["null_account_count"] - accounts_output_count, 0),
                source_accounts,
            ),
            "transactions_with_unknown_account_id": _metric(deduped_metrics["orphan_count"], source_transactions),
        },
    )
    set_raw_profile_section(
        "domain_drift",
        light_domain_drift(
            transaction_metrics,
            account_metrics,
            customer_metrics,
            source_transactions,
            source_accounts,
            source_customers,
            profile_settings,
        ),
    )
    set_raw_profile_section(
        "currency_readiness",
        currency_readiness_summary(transaction_metrics, source_transactions),
    )
    set_raw_profile_section(
        "geo_quality",
        geo_quality_summary(transaction_metrics, source_transactions),
    )


def run_transformation():
    config = load_config()
    rules = load_dq_rules()
    credibility_rules = load_credibility_rules(config)
    profile_settings = domain_drift_settings(credibility_rules)
    transaction_types = domain_values("transactions.transaction_type", credibility_rules, TRANSACTION_TYPES)
    transaction_channels = domain_values("transactions.channel", credibility_rules, TRANSACTION_CHANNELS)
    transaction_provinces = domain_values("transactions.province", credibility_rules, SA_PROVINCES)
    account_types = domain_values("accounts.account_type", credibility_rules, ACCOUNT_TYPES)
    account_statuses = domain_values("accounts.account_status", credibility_rules, ACCOUNT_STATUSES)
    product_tiers = domain_values("accounts.product_tier", credibility_rules, PRODUCT_TIERS)
    digital_channels = domain_values("accounts.digital_channel", credibility_rules, DIGITAL_CHANNELS)
    genders = domain_values("customers.gender", credibility_rules, GENDERS)
    customer_provinces = domain_values("customers.province", credibility_rules, SA_PROVINCES)
    kyc_statuses = domain_values("customers.kyc_status", credibility_rules, KYC_STATUSES)
    spark = spark_session(config)
    bronze_root = output_path(config, "bronze")
    silver_root = output_path(config, "silver")

    customers_b = read_delta(spark, f"{bronze_root}/customers")
    accounts_b = read_delta(spark, f"{bronze_root}/accounts")
    transactions_b = read_delta(spark, f"{bronze_root}/transactions")

    customers = (
        customers_b.withColumn("dob_parsed", parse_date(F.col("dob")))
        .withColumn("risk_score", F.col("risk_score").cast("int"))
        .withColumn("dob_date_variant", date_variant_flag(F.col("dob")))
    )
    customer_metrics = _aggregate_metrics(
        customers,
        source_count=_count_if(F.lit(True)),
        null_customer_count=_count_if(is_blank(F.col("customer_id"))),
        date_customer_count=_count_if(F.col("dob_date_variant")),
        dob_parse_failed_count=_count_if(~is_blank(F.col("dob")) & F.col("dob_parsed").isNull()),
        invalid_gender_count=_count_if(invalid_domain(F.col("gender"), genders)),
        invalid_customer_province_count=_count_if(invalid_domain(F.col("province"), customer_provinces)),
        invalid_kyc_status_count=_count_if(invalid_domain(F.col("kyc_status"), kyc_statuses)),
        gender_distinct_count=approx_distinct_values(F.col("gender")),
        gender_unknown_distinct_count=approx_distinct_unknown_values(F.col("gender"), genders),
        customer_province_distinct_count=approx_distinct_values(F.col("province")),
        customer_province_unknown_distinct_count=approx_distinct_unknown_values(F.col("province"), customer_provinces),
        kyc_status_distinct_count=approx_distinct_values(F.col("kyc_status")),
        kyc_status_unknown_distinct_count=approx_distinct_unknown_values(F.col("kyc_status"), kyc_statuses),
        risk_score_outside_range_count=_count_if(
            F.col("risk_score").isNotNull() & ((F.col("risk_score") < 1) | (F.col("risk_score") > 10))
        ),
        risk_score_quantiles=F.expr("percentile_approx(risk_score, array(0.5, 0.95, 0.99), 100)"),
    )
    date_customer_count = customer_metrics["date_customer_count"]
    customers = _dedupe(customers, "customer_id", ["customer_id"]).select(
        "customer_id",
        "id_number",
        "first_name",
        "last_name",
        F.col("dob_parsed").alias("dob"),
        "gender",
        "province",
        "income_band",
        "segment",
        "risk_score",
        "kyc_status",
        "product_flags",
        "ingestion_timestamp",
    )

    accounts = (
        accounts_b.withColumn("open_date_parsed", parse_date(F.col("open_date")))
        .withColumn("last_activity_date_parsed", parse_date(F.col("last_activity_date")))
        .withColumn("credit_limit", F.col("credit_limit").cast(T.DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(T.DecimalType(18, 2)))
        .withColumn("current_balance_double", F.col("current_balance").cast("double"))
        .withColumn(
            "date_variant",
            date_variant_flag(F.col("open_date"))
            | (F.col("last_activity_date").isNotNull() & date_variant_flag(F.col("last_activity_date"))),
        )
    )
    account_id_missing = F.col("account_id").isNull() | (F.trim(F.col("account_id")) == "")
    account_metrics = _aggregate_metrics(
        accounts,
        source_count=_count_if(F.lit(True)),
        null_account_count=_count_if(account_id_missing),
        null_customer_ref_count=_count_if(is_blank(F.col("customer_ref"))),
        date_account_count=_count_if(F.col("date_variant")),
        open_date_parse_failed_count=_count_if(~is_blank(F.col("open_date")) & F.col("open_date_parsed").isNull()),
        last_activity_before_open_count=_count_if(
            F.col("last_activity_date_parsed").isNotNull()
            & F.col("open_date_parsed").isNotNull()
            & (F.col("last_activity_date_parsed") < F.col("open_date_parsed"))
        ),
        invalid_account_type_count=_count_if(invalid_domain(F.col("account_type"), account_types)),
        invalid_account_status_count=_count_if(invalid_domain(F.col("account_status"), account_statuses)),
        invalid_product_tier_count=_count_if(invalid_domain(F.col("product_tier"), product_tiers)),
        invalid_digital_channel_count=_count_if(invalid_domain(F.col("digital_channel"), digital_channels)),
        account_type_distinct_count=approx_distinct_values(F.col("account_type")),
        account_type_unknown_distinct_count=approx_distinct_unknown_values(F.col("account_type"), account_types),
        account_status_distinct_count=approx_distinct_values(F.col("account_status")),
        account_status_unknown_distinct_count=approx_distinct_unknown_values(F.col("account_status"), account_statuses),
        product_tier_distinct_count=approx_distinct_values(F.col("product_tier")),
        product_tier_unknown_distinct_count=approx_distinct_unknown_values(F.col("product_tier"), product_tiers),
        digital_channel_distinct_count=approx_distinct_values(F.col("digital_channel")),
        digital_channel_unknown_distinct_count=approx_distinct_unknown_values(F.col("digital_channel"), digital_channels),
        negative_current_balance_count=_count_if(F.col("current_balance") < 0),
        negative_credit_limit_count=_count_if(F.col("credit_limit") < 0),
        current_balance_quantiles=F.expr("percentile_approx(current_balance_double, array(0.5, 0.95, 0.99), 100)"),
    )
    null_account_count = account_metrics["null_account_count"]
    date_account_count = account_metrics["date_account_count"]
    account_quarantine = accounts.where(account_id_missing)
    accounts_valid = accounts.where(~account_id_missing)
    accounts_valid = _dedupe(accounts_valid, "account_id", ["account_id"])
    accounts_valid = accounts_valid.join(F.broadcast(customers.select("customer_id")), accounts_valid.customer_ref == customers.customer_id, "inner")
    accounts_valid = accounts_valid.select(
        "account_id",
        "customer_ref",
        "account_type",
        "account_status",
        F.col("open_date_parsed").alias("open_date"),
        "product_tier",
        "mobile_number",
        "digital_channel",
        "credit_limit",
        "current_balance",
        F.col("last_activity_date_parsed").alias("last_activity_date"),
        F.col("ingestion_timestamp"),
    )
    accounts_valid = accounts_valid.persist(StorageLevel.MEMORY_AND_DISK)

    transactions = add_missing_columns(transactions_b, {"merchant_subcategory": T.StringType()})
    transactions = (
        transactions.withColumn("amount_decimal", F.col("amount").cast(T.DecimalType(18, 2)))
        .withColumn("amount_double", F.col("amount_decimal").cast("double"))
        .withColumn("transaction_date_parsed", parse_date(F.col("transaction_date")))
        .withColumn("transaction_timestamp", parse_timestamp(F.col("transaction_date"), F.col("transaction_time")))
        .withColumn("currency_clean", normalise_currency(F.col("currency")))
        .withColumn("province", F.col("location.province"))
        .withColumn("amount_type_mismatch", F.col("amount_was_quoted") & F.col("amount_decimal").isNotNull())
        .withColumn("amount_cast_failed", F.col("amount").isNotNull() & F.col("amount_decimal").isNull())
        .withColumn("transaction_date_variant", date_variant_flag(F.col("transaction_date")))
        .withColumn("currency_variant", currency_variant_flag(F.col("currency")))
    )
    transactions_profile = (
        transactions.withColumn("currency_raw", F.col("currency"))
        .withColumn("currency_iso_candidate", currency_iso_candidate(F.col("currency")))
        .withColumn("currency_numeric_candidate", currency_numeric_candidate(F.col("currency")))
        .withColumn("currency_conversion_required", currency_conversion_required(F.col("currency")))
        .withColumn("currency_conversion_ready", currency_conversion_ready(F.col("currency")))
        .withColumn("currency_unknown", currency_unknown(F.col("currency")))
        .withColumn("geo_coordinate_raw", F.col("location.coordinates"))
        .withColumn("geo_latitude", coordinate_latitude(F.col("location.coordinates")))
        .withColumn("geo_longitude", coordinate_longitude(F.col("location.coordinates")))
        .withColumn("geo_parse_failed", geo_parse_failed(F.col("geo_coordinate_raw"), F.col("geo_latitude"), F.col("geo_longitude")))
        .withColumn("geo_invalid_world_bounds", geo_invalid_world_bounds(F.col("geo_latitude"), F.col("geo_longitude")))
        .withColumn("geo_zero_zero", geo_zero_zero(F.col("geo_latitude"), F.col("geo_longitude")))
        .withColumn("geo_possible_swap", geo_possible_swap(F.col("geo_latitude"), F.col("geo_longitude")))
        .withColumn("geo_sa_sign_mismatch", geo_sa_sign_mismatch(F.col("geo_latitude"), F.col("geo_longitude")))
        .withColumn("geo_outside_sa_bounds", geo_outside_sa_bounds(F.col("geo_latitude"), F.col("geo_longitude")))
        .withColumn(
            "geo_province_mismatch",
            geo_province_mismatch(F.col("province"), F.col("geo_latitude"), F.col("geo_longitude"))
            if profile_settings.get("geo_province_coordinate_check_enabled", False)
            else F.lit(False),
        )
    )

    transaction_metrics = _aggregate_metrics(
        transactions_profile,
        source_count=_count_if(F.lit(True)),
        type_count=_count_if(F.col("amount_type_mismatch") | F.col("amount_cast_failed")),
        amount_cast_failed_count=_count_if(F.col("amount_cast_failed")),
        null_transaction_id=_count_if(is_blank(F.col("transaction_id"))),
        null_transaction_account_id=_count_if(is_blank(F.col("account_id"))),
        null_transaction_date=_count_if(is_blank(F.col("transaction_date"))),
        null_transaction_time=_count_if(is_blank(F.col("transaction_time"))),
        null_transaction_amount=_count_if(is_blank(F.col("amount"))),
        negative_amount_count=_count_if(F.col("amount_decimal") < 0),
        zero_amount_count=_count_if(F.col("amount_decimal") == 0),
        extreme_amount_count=_count_if(F.col("amount_decimal") > F.lit(50000)),
        amount_quantiles=F.expr("percentile_approx(amount_double, array(0.5, 0.95, 0.99), 100)"),
        date_transaction_count=_count_if(F.col("transaction_date_variant")),
        transaction_date_parse_failed_count=_count_if(~is_blank(F.col("transaction_date")) & F.col("transaction_date_parsed").isNull()),
        currency_count=_count_if(F.col("currency_variant")),
        invalid_currency_count=_count_if(F.col("currency_clean").isNotNull() & (F.col("currency_clean") != "ZAR")),
        currency_present_count=_count_if(~is_blank(F.col("currency_raw"))),
        currency_zar_like_count=_count_if(~is_blank(F.col("currency_raw")) & (F.col("currency_clean") == "ZAR")),
        currency_non_zar_like_count=_count_if(F.col("currency_conversion_required")),
        currency_unknown_count=_count_if(F.col("currency_unknown")),
        currency_iso_candidate_count=_count_if(F.col("currency_iso_candidate").isNotNull()),
        currency_numeric_candidate_count=_count_if(F.col("currency_numeric_candidate").isNotNull()),
        currency_conversion_required_count=_count_if(F.col("currency_conversion_required")),
        currency_conversion_ready_count=_count_if(F.col("currency_conversion_ready")),
        invalid_transaction_type_count=_count_if(invalid_domain(F.col("transaction_type"), transaction_types)),
        invalid_channel_count=_count_if(invalid_domain(F.col("channel"), transaction_channels)),
        invalid_transaction_province_count=_count_if(invalid_domain(F.col("province"), transaction_provinces)),
        transaction_type_distinct_count=approx_distinct_values(F.col("transaction_type")),
        transaction_type_unknown_distinct_count=approx_distinct_unknown_values(F.col("transaction_type"), transaction_types),
        channel_distinct_count=approx_distinct_values(F.col("channel")),
        channel_unknown_distinct_count=approx_distinct_unknown_values(F.col("channel"), transaction_channels),
        currency_distinct_count=approx_distinct_clean_values(F.col("currency_clean")),
        currency_unknown_distinct_count=approx_distinct_clean_unknown_values(F.col("currency_clean"), domain_values("transactions.currency", credibility_rules, {"ZAR"})),
        transaction_province_distinct_count=approx_distinct_values(F.col("province")),
        transaction_province_unknown_distinct_count=approx_distinct_unknown_values(F.col("province"), transaction_provinces),
        geo_coordinate_present_count=_count_if(~is_blank(F.col("geo_coordinate_raw"))),
        geo_coordinate_missing_count=_count_if(is_blank(F.col("geo_coordinate_raw"))),
        geo_coordinate_parse_failed_count=_count_if(F.col("geo_parse_failed")),
        geo_invalid_world_bounds_count=_count_if(F.col("geo_invalid_world_bounds")),
        geo_zero_zero_count=_count_if(F.col("geo_zero_zero")),
        geo_possible_swap_count=_count_if(F.col("geo_possible_swap")),
        geo_sa_sign_mismatch_count=_count_if(F.col("geo_sa_sign_mismatch")),
        geo_outside_sa_bounds_count=_count_if(F.col("geo_outside_sa_bounds")),
        geo_province_mismatch_count=_count_if(F.col("geo_province_mismatch")),
    )
    transaction_id_counts = transactions.groupBy("transaction_id").agg(F.count(F.lit(1)).cast("long").alias("_group_count"))
    transaction_group_metrics = _aggregate_metrics(
        transaction_id_counts,
        duplicate_count=F.sum(F.col("_group_count") - F.lit(1)).cast("long"),
    )
    source_transactions = METRICS["source_record_counts"].get("transactions_raw", transaction_metrics["source_count"])
    source_accounts = METRICS["source_record_counts"].get("accounts_raw", account_metrics["source_count"])
    source_customers = METRICS["source_record_counts"].get("customers_raw", customer_metrics["source_count"])
    source_all = source_transactions + source_accounts + source_customers
    duplicate_count = transaction_group_metrics["duplicate_count"]
    if duplicate_count:
        transactions_deduped = _dedupe_transactions_grouped(transactions).select("_picked.*")
    else:
        transactions_deduped = transactions

    required_missing = (
        F.col("transaction_id").isNull()
        | F.col("account_id").isNull()
        | F.col("transaction_date_parsed").isNull()
        | F.col("transaction_timestamp").isNull()
        | F.col("amount_decimal").isNull()
        | F.col("transaction_type").isNull()
        | F.col("currency_clean").isNull()
        | F.col("channel").isNull()
    )
    account_ids = F.broadcast(accounts_valid.select("account_id").withColumn("_account_exists", F.lit(True)))
    transactions_joined = transactions_deduped.join(account_ids, "account_id", "left")
    orphaned_condition = F.col("_account_exists").isNull()
    valid_transaction_condition = ~required_missing & ~orphaned_condition
    deduped_metrics = _aggregate_metrics(
        transactions_joined,
        orphan_count=_count_if(orphaned_condition),
        bad_required_count=_count_if(required_missing),
        type_output_count=_count_if(valid_transaction_condition & F.col("amount_type_mismatch")),
        currency_output_count=_count_if(
            valid_transaction_condition
            & ~F.col("amount_type_mismatch")
            & ~F.col("transaction_date_variant")
            & F.col("currency_variant")
        ),
    )
    orphan_count = deduped_metrics["orphan_count"]
    bad_required_count = deduped_metrics["bad_required_count"]
    bad_required = transactions_joined.where(required_missing).drop("_account_exists")
    orphaned = transactions_joined.where(orphaned_condition).drop("_account_exists")
    transactions_valid = transactions_joined.where(valid_transaction_condition).drop("_account_exists")

    type_count = transaction_metrics["type_count"]
    date_transaction_count = transaction_metrics["date_transaction_count"]
    currency_count = transaction_metrics["currency_count"]

    transactions_silver = (
        transactions_valid.withColumn(
            "dq_flag",
            F.when(F.col("amount_type_mismatch"), F.lit("TYPE_MISMATCH"))
            .when(F.col("transaction_date_variant"), F.lit("DATE_FORMAT"))
            .when(F.col("currency_variant"), F.lit("CURRENCY_VARIANT"))
            .otherwise(F.lit(None).cast("string")),
        )
    )
    transactions_silver = _select_transactions_silver(
        transactions_silver,
        include_provenance=profile_settings.get("enable_silver_provenance", False),
    )

    add_issue("DUPLICATE_DEDUPED", duplicate_count, source_transactions, _action("DUPLICATE_DEDUPED", rules), source_transactions - duplicate_count)
    add_issue("ORPHANED_ACCOUNT", orphan_count, source_transactions, _action("ORPHANED_ACCOUNT", rules), 0)
    add_issue("TYPE_MISMATCH", type_count, source_transactions, _action("TYPE_MISMATCH", rules), deduped_metrics["type_output_count"])
    add_issue("DATE_FORMAT", date_transaction_count + date_account_count + date_customer_count, source_all, _action("DATE_FORMAT", rules), date_transaction_count + date_account_count + date_customer_count)
    add_issue("CURRENCY_VARIANT", currency_count, source_transactions, _action("CURRENCY_VARIANT", rules), deduped_metrics["currency_output_count"])
    add_issue("NULL_REQUIRED", null_account_count, source_accounts, _action("NULL_REQUIRED", rules), 0)

    try:
        write_delta(customers, f"{silver_root}/customers")
        account_write_metrics = write_delta(accounts_valid, f"{silver_root}/accounts")
        write_delta(transactions_silver, f"{silver_root}/transactions")

        _set_light_raw_profile(
            source_transactions,
            source_accounts,
            source_customers,
            transaction_metrics,
            duplicate_count,
            deduped_metrics,
            account_metrics,
            customer_metrics,
            metric_int(account_write_metrics, "numOutputRows"),
            profile_settings,
        )

        quarantine_transactions = None
        if duplicate_count:
            duplicate_ids = transaction_id_counts.where(F.col("_group_count") > 1).select("transaction_id")
            quarantine_transactions = _quarantined_duplicates(transactions, duplicate_ids).select("raw_json")
        if orphan_count:
            orphan_rows = orphaned.select("raw_json")
            quarantine_transactions = orphan_rows if quarantine_transactions is None else quarantine_transactions.unionByName(orphan_rows, allowMissingColumns=True)
        if bad_required_count:
            bad_required_rows = bad_required.select("raw_json")
            quarantine_transactions = bad_required_rows if quarantine_transactions is None else quarantine_transactions.unionByName(bad_required_rows, allowMissingColumns=True)

        if quarantine_transactions is not None:
            write_delta(quarantine_transactions, f"{silver_root}/quarantine_transactions")
        if null_account_count:
            write_delta(account_quarantine, f"{silver_root}/quarantine_accounts")
    finally:
        accounts_valid.unpersist()
