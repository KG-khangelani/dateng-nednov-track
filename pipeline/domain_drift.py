from pyspark.sql import Window
from pyspark.sql import functions as F

from pipeline.common import normalise_currency
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
    domain_values,
    invalid_domain,
    is_blank,
    normalized,
)


DEFAULT_UNKNOWN_THRESHOLD = 50
DEFAULT_UNKNOWN_RATIO = 0.25
DEFAULT_TOP_K = 5


def approx_distinct_values(col):
    return F.approx_count_distinct(F.when(~is_blank(col), normalized(col)), 0.05).cast("long")


def approx_distinct_unknown_values(col, allowed_values):
    return F.approx_count_distinct(F.when(invalid_domain(col, allowed_values), normalized(col)), 0.05).cast("long")


def approx_distinct_clean_values(clean_col):
    return F.approx_count_distinct(F.when(clean_col.isNotNull(), clean_col), 0.05).cast("long")


def approx_distinct_clean_unknown_values(clean_col, allowed_values):
    return F.approx_count_distinct(
        F.when(clean_col.isNotNull() & ~clean_col.isin(*sorted(allowed_values)), clean_col),
        0.05,
    ).cast("long")


def _metric(count, total):
    count = int(count or 0)
    total = int(total or 0)
    return {
        "count": count,
        "pct": round(count * 100.0 / total, 2) if total else 0.0,
    }


def _nullable_int(value):
    if value is None:
        return None
    return int(value)


def _high_cardinality(unknown_distinct, distinct_values, threshold, ratio):
    if unknown_distinct is None:
        return False
    if unknown_distinct >= threshold:
        return True
    if distinct_values and unknown_distinct >= 3 and (unknown_distinct / distinct_values) >= ratio:
        return True
    return False


def _field_summary(total, allowed_values, invalid_count, distinct_values=None, unknown_distinct=None, top_unknown_values=None, settings=None, field=None):
    settings = settings or {}
    policy = ((settings.get("domains") or {}).get(field or "") or {})
    threshold = int(policy.get("high_cardinality_threshold", settings.get("domain_drift_unknown_threshold", DEFAULT_UNKNOWN_THRESHOLD)))
    ratio = float(settings.get("domain_drift_unknown_ratio", DEFAULT_UNKNOWN_RATIO))
    high_cardinality = _high_cardinality(unknown_distinct, distinct_values, threshold, ratio)
    suggestions = []
    if top_unknown_values and not high_cardinality:
        suggestions = [
            {
                "action": "review_allowed_value",
                "value": item["value"],
                "observed_count": int(item["count"]),
            }
            for item in top_unknown_values
        ]

    return {
        "known_allowed_value_count": len(allowed_values),
        "unknown_policy": policy.get("unknown_policy", "audit_only"),
        "invalid_values": _metric(invalid_count, total),
        "approx_distinct_values": _nullable_int(distinct_values),
        "approx_distinct_unknown_values": _nullable_int(unknown_distinct),
        "high_cardinality_drift": high_cardinality,
        "top_unknown_values": top_unknown_values or [],
        "candidate_rule_suggestions": suggestions,
    }


def _light_field(field, metrics, total, allowed_values, invalid_key, distinct_key, unknown_key, settings):
    return _field_summary(
        total=total,
        allowed_values=allowed_values,
        invalid_count=metrics.get(invalid_key, 0),
        distinct_values=metrics.get(distinct_key),
        unknown_distinct=metrics.get(unknown_key),
        settings=settings,
        field=field,
    )


def _allowed(field, fallback, settings=None):
    return domain_values(field, settings or {}, fallback)


def light_domain_drift(transaction_metrics, account_metrics, customer_metrics, source_transactions, source_accounts, source_customers, settings=None):
    settings = settings or {}
    if not settings.get("domain_drift_enabled", True):
        return {"enabled": False}

    return {
        "enabled": True,
        "mode": "light_aggregate_counts",
        "resource_guardrails": {
            "top_unknown_values_collected": False,
            "row_level_examples_collected": False,
            "note": "Light mode reuses transform aggregates and does not run high-cardinality value group-bys.",
        },
        "fields": {
            "transactions.transaction_type": _light_field(
                "transactions.transaction_type",
                transaction_metrics,
                source_transactions,
                _allowed("transactions.transaction_type", TRANSACTION_TYPES, settings),
                "invalid_transaction_type_count",
                "transaction_type_distinct_count",
                "transaction_type_unknown_distinct_count",
                settings,
            ),
            "transactions.channel": _light_field(
                "transactions.channel",
                transaction_metrics,
                source_transactions,
                _allowed("transactions.channel", TRANSACTION_CHANNELS, settings),
                "invalid_channel_count",
                "channel_distinct_count",
                "channel_unknown_distinct_count",
                settings,
            ),
            "transactions.currency": _light_field(
                "transactions.currency",
                transaction_metrics,
                source_transactions,
                _allowed("transactions.currency", {"ZAR"}, settings),
                "invalid_currency_count",
                "currency_distinct_count",
                "currency_unknown_distinct_count",
                settings,
            ),
            "transactions.province": _light_field(
                "transactions.province",
                transaction_metrics,
                source_transactions,
                _allowed("transactions.province", SA_PROVINCES, settings),
                "invalid_transaction_province_count",
                "transaction_province_distinct_count",
                "transaction_province_unknown_distinct_count",
                settings,
            ),
            "accounts.account_type": _light_field(
                "accounts.account_type",
                account_metrics,
                source_accounts,
                _allowed("accounts.account_type", ACCOUNT_TYPES, settings),
                "invalid_account_type_count",
                "account_type_distinct_count",
                "account_type_unknown_distinct_count",
                settings,
            ),
            "accounts.account_status": _light_field(
                "accounts.account_status",
                account_metrics,
                source_accounts,
                _allowed("accounts.account_status", ACCOUNT_STATUSES, settings),
                "invalid_account_status_count",
                "account_status_distinct_count",
                "account_status_unknown_distinct_count",
                settings,
            ),
            "accounts.product_tier": _light_field(
                "accounts.product_tier",
                account_metrics,
                source_accounts,
                _allowed("accounts.product_tier", PRODUCT_TIERS, settings),
                "invalid_product_tier_count",
                "product_tier_distinct_count",
                "product_tier_unknown_distinct_count",
                settings,
            ),
            "accounts.digital_channel": _light_field(
                "accounts.digital_channel",
                account_metrics,
                source_accounts,
                _allowed("accounts.digital_channel", DIGITAL_CHANNELS, settings),
                "invalid_digital_channel_count",
                "digital_channel_distinct_count",
                "digital_channel_unknown_distinct_count",
                settings,
            ),
            "customers.gender": _light_field(
                "customers.gender",
                customer_metrics,
                source_customers,
                _allowed("customers.gender", GENDERS, settings),
                "invalid_gender_count",
                "gender_distinct_count",
                "gender_unknown_distinct_count",
                settings,
            ),
            "customers.province": _light_field(
                "customers.province",
                customer_metrics,
                source_customers,
                _allowed("customers.province", SA_PROVINCES, settings),
                "invalid_customer_province_count",
                "customer_province_distinct_count",
                "customer_province_unknown_distinct_count",
                settings,
            ),
            "customers.kyc_status": _light_field(
                "customers.kyc_status",
                customer_metrics,
                source_customers,
                _allowed("customers.kyc_status", KYC_STATUSES, settings),
                "invalid_kyc_status_count",
                "kyc_status_distinct_count",
                "kyc_status_unknown_distinct_count",
                settings,
            ),
        },
    }


def _domain_specs(settings=None):
    settings = settings or {}
    return {
        "transactions": [
            {
                "field": "transactions.transaction_type",
                "column": F.col("transaction_type"),
                "allowed": _allowed("transactions.transaction_type", TRANSACTION_TYPES, settings),
            },
            {
                "field": "transactions.channel",
                "column": F.col("channel"),
                "allowed": _allowed("transactions.channel", TRANSACTION_CHANNELS, settings),
            },
            {
                "field": "transactions.currency",
                "column": F.col("currency"),
                "clean_column": normalise_currency(F.col("currency")),
                "allowed": _allowed("transactions.currency", {"ZAR"}, settings),
            },
            {
                "field": "transactions.province",
                "column": F.col("location.province"),
                "allowed": _allowed("transactions.province", SA_PROVINCES, settings),
            },
        ],
        "accounts": [
            {"field": "accounts.account_type", "column": F.col("account_type"), "allowed": _allowed("accounts.account_type", ACCOUNT_TYPES, settings)},
            {"field": "accounts.account_status", "column": F.col("account_status"), "allowed": _allowed("accounts.account_status", ACCOUNT_STATUSES, settings)},
            {"field": "accounts.product_tier", "column": F.col("product_tier"), "allowed": _allowed("accounts.product_tier", PRODUCT_TIERS, settings)},
            {"field": "accounts.digital_channel", "column": F.col("digital_channel"), "allowed": _allowed("accounts.digital_channel", DIGITAL_CHANNELS, settings)},
        ],
        "customers": [
            {"field": "customers.gender", "column": F.col("gender"), "allowed": _allowed("customers.gender", GENDERS, settings)},
            {"field": "customers.province", "column": F.col("province"), "allowed": _allowed("customers.province", SA_PROVINCES, settings)},
            {"field": "customers.kyc_status", "column": F.col("kyc_status"), "allowed": _allowed("customers.kyc_status", KYC_STATUSES, settings)},
        ],
    }


def _clean_col(spec):
    if "clean_column" in spec:
        clean_col = spec["clean_column"]
        if "column" in spec:
            return F.when(~is_blank(spec["column"]), clean_col)
        return clean_col
    return F.when(~is_blank(spec["column"]), normalized(spec["column"]))


def _invalid_expr(spec):
    clean_col = _clean_col(spec)
    if "clean_column" in spec:
        return clean_col.isNotNull() & ~clean_col.isin(*sorted(spec["allowed"]))
    return invalid_domain(spec["column"], spec["allowed"])


def _table_domain_metrics(df, specs):
    expressions = [F.count(F.lit(1)).cast("long").alias("_rows")]
    for index, spec in enumerate(specs):
        clean_col = _clean_col(spec)
        invalid_expr = _invalid_expr(spec)
        expressions.extend(
            [
                F.sum(F.when(invalid_expr, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(f"invalid_{index}"),
                F.approx_count_distinct(F.when(clean_col.isNotNull(), clean_col), 0.05).cast("long").alias(f"distinct_{index}"),
                F.approx_count_distinct(F.when(invalid_expr, clean_col), 0.05).cast("long").alias(f"unknown_distinct_{index}"),
            ]
        )
    return df.agg(*expressions).first()


def _top_unknown_values(df, specs, settings):
    if not settings.get("enable_top_k", True):
        return {}
    top_k = int(settings.get("domain_drift_top_k", settings.get("top_k", DEFAULT_TOP_K)))
    if top_k <= 0:
        return {}

    pairs = []
    for spec in specs:
        clean_col = _clean_col(spec)
        pairs.append(
            F.struct(
                F.lit(spec["field"]).alias("field"),
                F.when(_invalid_expr(spec), clean_col).alias("value"),
            )
        )
    if not pairs:
        return {}

    window = Window.partitionBy("field").orderBy(F.col("count").desc(), F.col("value").asc())
    rows = (
        df.select(F.explode(F.array(*pairs)).alias("kv"))
        .select("kv.field", "kv.value")
        .where(F.col("value").isNotNull())
        .groupBy("field", "value")
        .count()
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") <= top_k)
        .select("field", "value", "count")
        .collect()
    )
    result = {}
    for row in rows:
        result.setdefault(row["field"], []).append(
            {"value": row["value"], "count": int(row["count"])}
        )
    return result


def full_domain_drift(transactions, accounts, customers, settings=None):
    settings = settings or {}
    if not settings.get("domain_drift_enabled", True):
        return {"enabled": False}

    frames = {
        "transactions": transactions,
        "accounts": accounts,
        "customers": customers,
    }
    fields = {}
    for table_name, specs in _domain_specs(settings).items():
        row = _table_domain_metrics(frames[table_name], specs)
        top_unknown = _top_unknown_values(frames[table_name], specs, settings)
        total = int(row["_rows"] or 0)
        for index, spec in enumerate(specs):
            fields[spec["field"]] = _field_summary(
                total=total,
                allowed_values=spec["allowed"],
                invalid_count=row[f"invalid_{index}"],
                distinct_values=row[f"distinct_{index}"],
                unknown_distinct=row[f"unknown_distinct_{index}"],
                top_unknown_values=top_unknown.get(spec["field"], []),
                settings=settings,
                field=spec["field"],
            )

    return {
        "enabled": True,
        "mode": "full_aggregate_counts_with_capped_unknown_top_k",
        "resource_guardrails": {
            "top_unknown_values_collected": bool(settings.get("enable_top_k", True)),
            "top_k_unknown_values": int(settings.get("domain_drift_top_k", settings.get("top_k", DEFAULT_TOP_K))),
            "row_level_examples_collected": False,
            "note": "Only category values and counts are emitted; no sensitive row dumps are collected.",
        },
        "fields": fields,
    }
