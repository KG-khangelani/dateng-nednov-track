import json
import os
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import yaml
from pyspark.sql import Window
from pyspark.sql import functions as F

from pipeline.common import (
    PROJECT_ROOT,
    RUN_TIMESTAMP,
    load_config,
    normalise_currency,
    output_path,
    parse_date,
    parse_timestamp,
    read_delta,
    spark_session,
)
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
    advisory_rule_catalog,
    count_if,
    invalid_domain,
    is_blank,
    normalized,
    trimmed,
)


DEFAULT_CREDIBILITY_RULES = "/data/config/credibility_rules.yaml"
DEFAULT_PROFILE_OUTPUT = "/data/output/audit/raw_anomaly_profile.json"


def _existing_path(*candidates):
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return str(candidate)
    return str(candidates[0])


def load_credibility_rules(config=None):
    config = config or load_config()
    configured = (config.get("credibility") or {}).get("rules_path")
    path = _existing_path(
        os.environ.get("CREDIBILITY_RULES_CONFIG", configured or DEFAULT_CREDIBILITY_RULES),
        PROJECT_ROOT / "config" / "credibility_rules.yaml",
    )
    if not Path(path).exists():
        return {"profile": {}, "advisory_rules": {}}
    with open(path, "r", encoding="utf-8") as handle:
        rules = yaml.safe_load(handle) or {}
    rules["_rules_path"] = path
    return rules


def _settings(rules):
    profile = rules.get("profile") or {}
    return {
        "enabled": bool(profile.get("enabled", True)),
        "output_path": profile.get("output_path", DEFAULT_PROFILE_OUTPUT),
        "top_k": int(profile.get("top_k", 5)),
        "enable_top_k": bool(profile.get("enable_top_k", True)),
        "enable_referential_checks": bool(profile.get("enable_referential_checks", True)),
        "enable_province_match_check": bool(profile.get("enable_province_match_check", True)),
        "enable_repeat_pattern_checks": bool(profile.get("enable_repeat_pattern_checks", False)),
        "repeat_pattern_threshold": int(profile.get("repeat_pattern_threshold", 50)),
        "percentile_accuracy": int(profile.get("percentile_accuracy", 100)),
        "extreme_amount_threshold": float(profile.get("extreme_amount_threshold", 50000)),
        "extreme_balance_threshold": float(profile.get("extreme_balance_threshold", 250000)),
    }


def _json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if hasattr(value, "asDict"):
        return _json_safe(value.asDict(recursive=True))
    return value


def _metric(count, total):
    count = int(count or 0)
    total = int(total or 0)
    return {
        "count": count,
        "pct": round(count * 100.0 / total, 2) if total else 0.0,
    }


def _first_aggregate(df, **expressions):
    row = df.agg(*[expr.alias(name) for name, expr in expressions.items()]).first()
    return {name: _json_safe(row[name]) for name in expressions}


def _distinct(col):
    return F.countDistinct(F.when(~is_blank(col), trimmed(col)))


def _duplicate_surplus(row_count, distinct_count):
    return max(int(row_count or 0) - int(distinct_count or 0), 0)


def _with_top_k(profile, df, field_expressions, settings):
    if not settings["enable_top_k"] or settings["top_k"] <= 0:
        profile["top_k"] = {}
        return

    pairs = [
        F.struct(
            F.lit(name).alias("field"),
            F.coalesce(trimmed(expr), F.lit("<NULL>")).alias("value"),
        )
        for name, expr in field_expressions
    ]
    if not pairs:
        profile["top_k"] = {}
        return

    window = Window.partitionBy("field").orderBy(F.col("count").desc(), F.col("value").asc())
    rows = (
        df.select(F.explode(F.array(*pairs)).alias("kv"))
        .select("kv.field", "kv.value")
        .groupBy("field", "value")
        .count()
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") <= settings["top_k"])
        .select("field", "value", "count")
        .collect()
    )

    top_k = {}
    for row in rows:
        top_k.setdefault(row["field"], []).append(
            {"value": row["value"], "count": int(row["count"])}
        )
    profile["top_k"] = top_k


def _transaction_profile(transactions, settings):
    amount = F.col("_amount_double")
    transaction_date = F.col("_transaction_date_parsed")
    timestamp = F.col("_transaction_timestamp_parsed")
    province = F.col("location.province")
    currency_normalized = normalized(F.col("currency"))
    currency_clean = normalise_currency(F.col("currency"))

    prepared = (
        transactions.withColumn("_amount_double", F.col("amount").cast("double"))
        .withColumn("_transaction_date_parsed", parse_date(F.col("transaction_date")))
        .withColumn("_transaction_timestamp_parsed", parse_timestamp(F.col("transaction_date"), F.col("transaction_time")))
    )

    row = _first_aggregate(
        prepared,
        rows=F.count(F.lit(1)),
        distinct_transaction_ids=_distinct(F.col("transaction_id")),
        null_transaction_id=count_if(is_blank(F.col("transaction_id"))),
        null_account_id=count_if(is_blank(F.col("account_id"))),
        null_transaction_date=count_if(is_blank(F.col("transaction_date"))),
        null_transaction_time=count_if(is_blank(F.col("transaction_time"))),
        null_amount=count_if(is_blank(F.col("amount"))),
        amount_parse_failed=count_if(~is_blank(F.col("amount")) & amount.isNull()),
        negative_amount=count_if(amount < 0),
        zero_amount=count_if(amount == 0),
        extreme_amount=count_if(amount > F.lit(settings["extreme_amount_threshold"])),
        amount_quantiles=F.expr(
            f"percentile_approx(_amount_double, array(0.5, 0.95, 0.99), {settings['percentile_accuracy']})"
        ),
        invalid_transaction_type=count_if(invalid_domain(F.col("transaction_type"), TRANSACTION_TYPES)),
        invalid_channel=count_if(invalid_domain(F.col("channel"), TRANSACTION_CHANNELS)),
        date_parse_failed=count_if(~is_blank(F.col("transaction_date")) & transaction_date.isNull()),
        timestamp_parse_failed=count_if(~is_blank(F.col("transaction_time")) & timestamp.isNull()),
        future_transaction_date=count_if(transaction_date > F.current_date()),
        currency_variant=count_if(~is_blank(F.col("currency")) & (currency_normalized != "ZAR") & (currency_clean == "ZAR")),
        invalid_currency=count_if(~is_blank(F.col("currency")) & currency_clean.isNotNull() & (currency_clean != "ZAR")),
        null_province=count_if(is_blank(province)),
        invalid_province=count_if(invalid_domain(province, SA_PROVINCES)),
        merchant_subcategory_missing=count_if(is_blank(F.col("merchant_subcategory"))),
        retry_flagged=count_if(F.col("metadata.retry_flag") == F.lit(True)),
    )

    total = row["rows"]
    profile = {
        "rows": int(total),
        "distinct_transaction_ids": int(row["distinct_transaction_ids"] or 0),
        "duplicate_transaction_surplus": _metric(
            _duplicate_surplus(total, row["distinct_transaction_ids"]),
            total,
        ),
        "required_nulls": {
            "transaction_id": _metric(row["null_transaction_id"], total),
            "account_id": _metric(row["null_account_id"], total),
            "transaction_date": _metric(row["null_transaction_date"], total),
            "transaction_time": _metric(row["null_transaction_time"], total),
            "amount": _metric(row["null_amount"], total),
        },
        "amount_quality": {
            "parse_failed": _metric(row["amount_parse_failed"], total),
            "negative": _metric(row["negative_amount"], total),
            "zero": _metric(row["zero_amount"], total),
            "above_configured_extreme_threshold": _metric(row["extreme_amount"], total),
            "quantiles": row["amount_quantiles"] or [],
        },
        "domain_quality": {
            "invalid_transaction_type": _metric(row["invalid_transaction_type"], total),
            "invalid_channel": _metric(row["invalid_channel"], total),
            "currency_variant": _metric(row["currency_variant"], total),
            "invalid_currency": _metric(row["invalid_currency"], total),
            "null_province": _metric(row["null_province"], total),
            "invalid_province": _metric(row["invalid_province"], total),
            "merchant_subcategory_missing": _metric(row["merchant_subcategory_missing"], total),
        },
        "temporal_quality": {
            "date_parse_failed": _metric(row["date_parse_failed"], total),
            "timestamp_parse_failed": _metric(row["timestamp_parse_failed"], total),
            "future_transaction_date": _metric(row["future_transaction_date"], total),
        },
        "operational_signals": {
            "retry_flagged": _metric(row["retry_flagged"], total),
        },
    }
    _with_top_k(
        profile,
        prepared,
        [
            ("transaction_type", F.col("transaction_type")),
            ("channel", F.col("channel")),
            ("currency", F.col("currency")),
            ("province", F.col("location.province")),
        ],
        settings,
    )
    return profile


def _account_profile(accounts, settings):
    open_date = F.col("_open_date_parsed")
    last_activity = F.col("_last_activity_date_parsed")
    credit_limit = F.col("_credit_limit_double")
    current_balance = F.col("_current_balance_double")
    account_type = normalized(F.col("account_type"))

    prepared = (
        accounts.withColumn("_open_date_parsed", parse_date(F.col("open_date")))
        .withColumn("_last_activity_date_parsed", parse_date(F.col("last_activity_date")))
        .withColumn("_credit_limit_double", F.col("credit_limit").cast("double"))
        .withColumn("_current_balance_double", F.col("current_balance").cast("double"))
    )

    row = _first_aggregate(
        prepared,
        rows=F.count(F.lit(1)),
        distinct_account_ids=_distinct(F.col("account_id")),
        null_account_id=count_if(is_blank(F.col("account_id"))),
        null_customer_ref=count_if(is_blank(F.col("customer_ref"))),
        invalid_account_type=count_if(invalid_domain(F.col("account_type"), ACCOUNT_TYPES)),
        invalid_account_status=count_if(invalid_domain(F.col("account_status"), ACCOUNT_STATUSES)),
        invalid_product_tier=count_if(invalid_domain(F.col("product_tier"), PRODUCT_TIERS)),
        invalid_digital_channel=count_if(invalid_domain(F.col("digital_channel"), DIGITAL_CHANNELS)),
        open_date_parse_failed=count_if(~is_blank(F.col("open_date")) & open_date.isNull()),
        last_activity_date_parse_failed=count_if(~is_blank(F.col("last_activity_date")) & last_activity.isNull()),
        future_open_date=count_if(open_date > F.current_date()),
        last_activity_before_open=count_if(last_activity.isNotNull() & open_date.isNotNull() & (last_activity < open_date)),
        current_balance_parse_failed=count_if(~is_blank(F.col("current_balance")) & current_balance.isNull()),
        negative_current_balance=count_if(current_balance < 0),
        extreme_current_balance=count_if(F.abs(current_balance) > F.lit(settings["extreme_balance_threshold"])),
        credit_limit_parse_failed=count_if(~is_blank(F.col("credit_limit")) & credit_limit.isNull()),
        negative_credit_limit=count_if(credit_limit < 0),
        non_credit_has_credit_limit=count_if((account_type != "CREDIT") & ~is_blank(F.col("credit_limit"))),
        credit_missing_credit_limit=count_if((account_type == "CREDIT") & (credit_limit.isNull() | (credit_limit <= 0))),
        current_balance_quantiles=F.expr(
            f"percentile_approx(_current_balance_double, array(0.5, 0.95, 0.99), {settings['percentile_accuracy']})"
        ),
    )

    total = row["rows"]
    profile = {
        "rows": int(total),
        "distinct_account_ids": int(row["distinct_account_ids"] or 0),
        "duplicate_account_surplus": _metric(
            _duplicate_surplus(total, row["distinct_account_ids"]),
            total,
        ),
        "required_nulls": {
            "account_id": _metric(row["null_account_id"], total),
            "customer_ref": _metric(row["null_customer_ref"], total),
        },
        "domain_quality": {
            "invalid_account_type": _metric(row["invalid_account_type"], total),
            "invalid_account_status": _metric(row["invalid_account_status"], total),
            "invalid_product_tier": _metric(row["invalid_product_tier"], total),
            "invalid_digital_channel": _metric(row["invalid_digital_channel"], total),
        },
        "temporal_quality": {
            "open_date_parse_failed": _metric(row["open_date_parse_failed"], total),
            "last_activity_date_parse_failed": _metric(row["last_activity_date_parse_failed"], total),
            "future_open_date": _metric(row["future_open_date"], total),
            "last_activity_before_open": _metric(row["last_activity_before_open"], total),
        },
        "numeric_quality": {
            "current_balance_parse_failed": _metric(row["current_balance_parse_failed"], total),
            "negative_current_balance": _metric(row["negative_current_balance"], total),
            "extreme_current_balance": _metric(row["extreme_current_balance"], total),
            "credit_limit_parse_failed": _metric(row["credit_limit_parse_failed"], total),
            "negative_credit_limit": _metric(row["negative_credit_limit"], total),
            "non_credit_has_credit_limit": _metric(row["non_credit_has_credit_limit"], total),
            "credit_missing_or_non_positive_credit_limit": _metric(row["credit_missing_credit_limit"], total),
            "current_balance_quantiles": row["current_balance_quantiles"] or [],
        },
    }
    _with_top_k(
        profile,
        prepared,
        [
            ("account_type", F.col("account_type")),
            ("account_status", F.col("account_status")),
            ("product_tier", F.col("product_tier")),
            ("digital_channel", F.col("digital_channel")),
        ],
        settings,
    )
    return profile


def _customer_profile(customers, settings):
    dob = F.col("_dob_parsed")
    risk_score = F.col("_risk_score_int")
    age_years = F.floor(F.months_between(F.current_date(), dob) / F.lit(12))

    prepared = (
        customers.withColumn("_dob_parsed", parse_date(F.col("dob")))
        .withColumn("_risk_score_int", F.col("risk_score").cast("int"))
    )

    row = _first_aggregate(
        prepared,
        rows=F.count(F.lit(1)),
        distinct_customer_ids=_distinct(F.col("customer_id")),
        null_customer_id=count_if(is_blank(F.col("customer_id"))),
        invalid_gender=count_if(invalid_domain(F.col("gender"), GENDERS)),
        invalid_province=count_if(invalid_domain(F.col("province"), SA_PROVINCES)),
        invalid_kyc_status=count_if(invalid_domain(F.col("kyc_status"), KYC_STATUSES)),
        dob_parse_failed=count_if(~is_blank(F.col("dob")) & dob.isNull()),
        future_dob=count_if(dob > F.current_date()),
        under_18=count_if(dob.isNotNull() & (age_years < 18)),
        over_110=count_if(dob.isNotNull() & (age_years > 110)),
        risk_score_parse_failed=count_if(~is_blank(F.col("risk_score")) & risk_score.isNull()),
        risk_score_outside_range=count_if(risk_score.isNotNull() & ((risk_score < 1) | (risk_score > 10))),
        missing_id_number=count_if(is_blank(F.col("id_number"))),
        missing_first_name=count_if(is_blank(F.col("first_name"))),
        missing_last_name=count_if(is_blank(F.col("last_name"))),
        missing_product_flags=count_if(is_blank(F.col("product_flags"))),
        risk_score_quantiles=F.expr(
            f"percentile_approx(_risk_score_int, array(0.5, 0.95, 0.99), {settings['percentile_accuracy']})"
        ),
    )

    total = row["rows"]
    profile = {
        "rows": int(total),
        "distinct_customer_ids": int(row["distinct_customer_ids"] or 0),
        "duplicate_customer_surplus": _metric(
            _duplicate_surplus(total, row["distinct_customer_ids"]),
            total,
        ),
        "required_nulls": {
            "customer_id": _metric(row["null_customer_id"], total),
        },
        "domain_quality": {
            "invalid_gender": _metric(row["invalid_gender"], total),
            "invalid_province": _metric(row["invalid_province"], total),
            "invalid_kyc_status": _metric(row["invalid_kyc_status"], total),
        },
        "temporal_quality": {
            "dob_parse_failed": _metric(row["dob_parse_failed"], total),
            "future_dob": _metric(row["future_dob"], total),
            "under_18": _metric(row["under_18"], total),
            "over_110": _metric(row["over_110"], total),
        },
        "numeric_quality": {
            "risk_score_parse_failed": _metric(row["risk_score_parse_failed"], total),
            "risk_score_outside_range": _metric(row["risk_score_outside_range"], total),
            "risk_score_quantiles": row["risk_score_quantiles"] or [],
        },
        "profile_completeness": {
            "missing_id_number": _metric(row["missing_id_number"], total),
            "missing_first_name": _metric(row["missing_first_name"], total),
            "missing_last_name": _metric(row["missing_last_name"], total),
            "missing_product_flags": _metric(row["missing_product_flags"], total),
        },
    }
    _with_top_k(
        profile,
        prepared,
        [
            ("province", F.col("province")),
            ("gender", F.col("gender")),
            ("kyc_status", F.col("kyc_status")),
            ("segment", F.col("segment")),
        ],
        settings,
    )
    return profile


def _cross_table_profile(transactions, accounts, customers, table_profiles, settings):
    if not settings["enable_referential_checks"]:
        return {"referential_checks_enabled": False}

    accounts_total = table_profiles["accounts"]["rows"]
    transactions_total = table_profiles["transactions"]["rows"]
    account_keys = (
        accounts.where(~is_blank(F.col("account_id")))
        .select(trimmed(F.col("account_id")).alias("account_id"))
        .dropDuplicates()
    )
    customer_keys = (
        customers.where(~is_blank(F.col("customer_id")))
        .select(trimmed(F.col("customer_id")).alias("customer_id"))
        .dropDuplicates()
    )

    orphan_accounts = (
        accounts.where(~is_blank(F.col("customer_ref")))
        .select(trimmed(F.col("customer_ref")).alias("customer_ref"))
        .join(F.broadcast(customer_keys), F.col("customer_ref") == customer_keys.customer_id, "left_anti")
        .count()
    )
    orphan_transactions = (
        transactions.where(~is_blank(F.col("account_id")))
        .select(trimmed(F.col("account_id")).alias("account_id"))
        .join(F.broadcast(account_keys), "account_id", "left_anti")
        .count()
    )

    result = {
        "referential_checks_enabled": True,
        "accounts_with_unknown_customer_ref": _metric(orphan_accounts, accounts_total),
        "transactions_with_unknown_account_id": _metric(orphan_transactions, transactions_total),
    }

    if settings["enable_province_match_check"]:
        account_customer = (
            accounts.where(~is_blank(F.col("account_id")) & ~is_blank(F.col("customer_ref")))
            .select(
                trimmed(F.col("account_id")).alias("account_id"),
                trimmed(F.col("customer_ref")).alias("customer_id"),
            )
            .join(
                customers.select(
                    trimmed(F.col("customer_id")).alias("customer_id"),
                    trimmed(F.col("province")).alias("customer_province"),
                ),
                "customer_id",
                "inner",
            )
            .select("account_id", "customer_province")
            .dropDuplicates(["account_id"])
        )
        province_mismatch = (
            transactions.where(~is_blank(F.col("account_id")) & ~is_blank(F.col("location.province")))
            .select(
                trimmed(F.col("account_id")).alias("account_id"),
                trimmed(F.col("location.province")).alias("transaction_province"),
            )
            .join(F.broadcast(account_customer), "account_id", "inner")
            .where(F.upper(F.col("transaction_province")) != F.upper(F.col("customer_province")))
            .count()
        )
        result["transactions_with_customer_province_mismatch"] = _metric(
            province_mismatch,
            transactions_total,
        )

    if settings["enable_repeat_pattern_checks"]:
        repeat_threshold = settings["repeat_pattern_threshold"]
        grouped = (
            transactions.where(~is_blank(F.col("account_id")))
            .groupBy("account_id", "transaction_type", "channel")
            .count()
        )
        row = _first_aggregate(
            grouped,
            groups_at_or_above_threshold=count_if(F.col("count") >= F.lit(repeat_threshold)),
            max_group_count=F.max("count"),
        )
        result["dense_account_type_channel_patterns"] = {
            "threshold": repeat_threshold,
            "groups_at_or_above_threshold": int(row["groups_at_or_above_threshold"] or 0),
            "max_group_count": int(row["max_group_count"] or 0),
            "note": "No account_id values are emitted.",
        }

    return result


def _write_json(path, report):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as handle:
        json.dump(_json_safe(report), handle, indent=2, sort_keys=False)


def run_raw_profile():
    config = load_config()
    rules = load_credibility_rules(config)
    settings = _settings(rules)
    if not settings["enabled"]:
        return

    started_at = time.time()
    spark = spark_session(config)
    bronze_root = output_path(config, "bronze")
    accounts = read_delta(spark, f"{bronze_root}/accounts")
    customers = read_delta(spark, f"{bronze_root}/customers")
    transactions = read_delta(spark, f"{bronze_root}/transactions")

    section_durations = {}
    section_started = time.time()
    table_profiles = {"transactions": _transaction_profile(transactions, settings)}
    section_durations["transactions"] = int(time.time() - section_started)

    section_started = time.time()
    table_profiles["accounts"] = _account_profile(accounts, settings)
    section_durations["accounts"] = int(time.time() - section_started)

    section_started = time.time()
    table_profiles["customers"] = _customer_profile(customers, settings)
    section_durations["customers"] = int(time.time() - section_started)

    section_started = time.time()
    cross_table = _cross_table_profile(transactions, accounts, customers, table_profiles, settings)
    section_durations["cross_table"] = int(time.time() - section_started)

    profile = {
        "$schema": "nedbank-de-challenge/raw-anomaly-profile/v1",
        "run_timestamp": RUN_TIMESTAMP,
        "profile_mode": "rough_aggregate_only",
        "rules_path": rules.get("_rules_path"),
        "settings": {
            key: value
            for key, value in settings.items()
            if key not in {"output_path"}
        },
        "advisory_rule_catalog": advisory_rule_catalog(),
        "tables": table_profiles,
        "cross_table": cross_table,
        "performance_observation": {
            "profiling_duration_seconds": int(time.time() - started_at),
            "section_duration_seconds": section_durations,
            "cpu_note": "Designed to use bounded Spark aggregations on local[2]; disable top_k or repeat pattern checks if CPU tails become too long.",
            "memory_note": "No pandas conversion or row-level raw samples are collected.",
        },
    }
    _write_json(settings["output_path"], profile)
