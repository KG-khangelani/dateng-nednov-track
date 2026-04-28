import json
import math
import os
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.common import (
    RUN_TIMESTAMP,
    add_missing_columns,
    currency_variant_flag,
    date_variant_flag,
    infer_stage,
    load_config,
    load_dq_rules,
    metric_int,
    normalise_currency,
    output_path,
    parse_date,
    parse_timestamp,
    raw_transaction_frame,
    read_delta,
    spark_session,
    write_delta,
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
    invalid_domain,
    is_blank,
)
from pipeline.domain_drift import approx_distinct_unknown_values, approx_distinct_values
from pipeline.ingest import _read_csv_raw, _run_timestamp_col
from pipeline.metrics import METRICS, add_issue, set_gold_count, set_raw_profile_section, set_source_count
from pipeline.provision import _age_band, _with_surrogate_key, _write_report_if_needed
from pipeline.transform import (
    _action,
    _aggregate_metrics,
    _count_if,
    _dedupe,
    _dedupe_transactions_grouped,
    _metric,
    _quarantined_duplicates,
    _set_light_raw_profile,
)


DEFAULT_MAX_BUCKET_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_BUCKET_COUNT = 256
DEFAULT_SCHEDULE_STRATEGY = "resume_failed_largest_first"
SAMPLE_LINES = 10000


def _read_int(path):
    try:
        value = Path(path).read_text(encoding="utf-8").strip()
        if value and value != "max":
            return int(value)
    except (FileNotFoundError, OSError, ValueError):
        return None
    return None


def _cpu_quota_cores():
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text(encoding="utf-8").split()
        if quota != "max":
            return round(int(quota) / int(period), 2)
    except (FileNotFoundError, OSError, ValueError):
        pass
    quota = _read_int("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    period = _read_int("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if quota and quota > 0 and period:
        return round(quota / period, 2)
    return None


def _file_fingerprint(path):
    file_path = Path(path)
    try:
        stat = file_path.stat()
        return {
            "path": str(file_path),
            "bytes": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }
    except OSError:
        return {
            "path": str(file_path),
            "bytes": 0,
            "mtime_ns": None,
            "missing": True,
        }


def _estimate_jsonl_rows(path, file_bytes):
    if not file_bytes:
        return 0
    sampled_bytes = 0
    sampled_rows = 0
    try:
        with open(path, "rb") as handle:
            for line in handle:
                sampled_bytes += len(line)
                sampled_rows += 1
                if sampled_rows >= SAMPLE_LINES:
                    break
    except OSError:
        return None
    if sampled_rows == 0 or sampled_bytes == 0:
        return 0
    return int(file_bytes / (sampled_bytes / sampled_rows))


def build_preflight_plan(config=None):
    config = config or load_config()
    runtime = config.get("runtime") or {}
    adaptive = runtime.get("adaptive") or {}
    mode = os.environ.get("PIPELINE_RUNTIME_MODE", runtime.get("mode", "scorer")).strip().lower()
    if mode not in {"scorer", "adaptive"}:
        mode = "scorer"

    input_config = config.get("input") or {}
    fingerprints = {
        "accounts": _file_fingerprint(input_config.get("accounts_path", "")),
        "customers": _file_fingerprint(input_config.get("customers_path", "")),
        "transactions": _file_fingerprint(input_config.get("transactions_path", "")),
    }
    transaction_bytes = fingerprints["transactions"]["bytes"]
    configured_bucket_count = adaptive.get("bucket_count")
    max_bucket_bytes = int(os.environ.get("ADAPTIVE_MAX_BUCKET_BYTES", adaptive.get("max_bucket_bytes", DEFAULT_MAX_BUCKET_BYTES)))
    max_bucket_count = int(adaptive.get("max_bucket_count", DEFAULT_MAX_BUCKET_COUNT))
    if configured_bucket_count:
        bucket_count = max(1, int(configured_bucket_count))
        bucket_source = "configured"
    else:
        bucket_count = max(1, int(math.ceil(transaction_bytes / max_bucket_bytes))) if max_bucket_bytes > 0 else 1
        bucket_source = "derived_from_input_bytes"

    tier = "scorer"
    if mode == "adaptive":
        tier = "large" if bucket_count > 1 else "medium"

    safe_to_run = True
    failure_reason = None
    if mode == "adaptive" and bucket_count > max_bucket_count:
        safe_to_run = False
        failure_reason = f"derived bucket_count {bucket_count} exceeds max_bucket_count {max_bucket_count}"

    return {
        "mode": mode,
        "tier": tier,
        "safe_to_run": safe_to_run,
        "failure_reason": failure_reason,
        "bucket_count": bucket_count,
        "bucket_count_source": bucket_source,
        "max_bucket_bytes": max_bucket_bytes,
        "max_bucket_count": max_bucket_count,
        "schedule_strategy": adaptive.get("schedule_strategy", DEFAULT_SCHEDULE_STRATEGY),
        "estimated_transaction_rows": _estimate_jsonl_rows(input_config.get("transactions_path", ""), transaction_bytes),
        "input_fingerprints": fingerprints,
        "manifest_path": adaptive.get("manifest_path", "/data/output/audit/chunk_manifest.json"),
        "resource_limits": {
            "memory_limit_bytes": _read_int("/sys/fs/cgroup/memory.max")
            or _read_int("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
            "cpu_quota_cores": _cpu_quota_cores(),
        },
    }


def is_adaptive_large(preflight):
    return (preflight or {}).get("mode") == "adaptive" and (preflight or {}).get("tier") == "large"


def bucket_expr(transaction_id_col, bucket_count):
    bucket_key = F.coalesce(transaction_id_col.cast("string"), F.lit("__NULL_TRANSACTION_ID__"))
    return F.pmod(F.xxhash64(bucket_key), F.lit(int(bucket_count))).cast("int")


def add_processing_bucket(df, bucket_count):
    return df.withColumn("_processing_bucket", bucket_expr(F.col("transaction_id"), bucket_count))


def _empty_manifest(preflight):
    return {
        "$schema": "nedbank-de-challenge/adaptive-chunk-manifest/v1",
        "run_timestamp": RUN_TIMESTAMP,
        "bucket_count": int(preflight["bucket_count"]),
        "input_fingerprints": preflight["input_fingerprints"],
        "completed_buckets": [],
        "failed_buckets": [],
        "active_bucket": None,
        "bucket_metrics": {},
        "finalization_status": "not_started",
        "finalized": False,
    }


def _manifest_matches(manifest, preflight):
    return (
        manifest.get("bucket_count") == int(preflight["bucket_count"])
        and manifest.get("input_fingerprints") == preflight.get("input_fingerprints")
    )


def _load_manifest(preflight):
    path = Path(preflight["manifest_path"])
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            if _manifest_matches(manifest, preflight):
                return manifest
        except (OSError, json.JSONDecodeError):
            pass
    return _empty_manifest(preflight)


def _write_manifest(preflight, manifest):
    path = Path(preflight["manifest_path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=False)
    os.replace(tmp_path, path)


def _delta_exists(path):
    return (Path(path) / "_delta_log").exists()


def _finalized_outputs_exist(config, manifest):
    bronze_root = output_path(config, "bronze")
    silver_root = output_path(config, "silver")
    gold_root = output_path(config, "gold")
    required_delta_paths = [
        f"{bronze_root}/accounts",
        f"{bronze_root}/customers",
        f"{bronze_root}/transactions",
        f"{silver_root}/customers",
        f"{silver_root}/accounts",
        f"{silver_root}/transactions",
        f"{gold_root}/dim_customers",
        f"{gold_root}/dim_accounts",
        f"{gold_root}/fact_transactions",
    ]
    for bucket_id in manifest.get("completed_buckets", []):
        required_delta_paths.append(_bucket_dir(silver_root, "transactions", bucket_id))
        required_delta_paths.append(_bucket_dir(gold_root, "fact_transactions", bucket_id))
    if not all(_delta_exists(path) for path in required_delta_paths):
        return False

    raw_profile_path = Path(gold_root).parent / "audit" / "raw_anomaly_profile.json"
    if not raw_profile_path.exists():
        return False
    try:
        with open(raw_profile_path, "r", encoding="utf-8") as handle:
            raw_profile = json.load(handle)
        if not raw_profile.get("domain_drift"):
            return False
    except (OSError, json.JSONDecodeError):
        return False

    if infer_stage(config) != "1" and not Path(config["output"]["dq_report_path"]).exists():
        return False
    return True


def _bucket_dir(root, table, bucket_id):
    return f"{root}/{table}_chunks/bucket={int(bucket_id):05d}"


def _int_set(values):
    result = set()
    for value in values or []:
        try:
            result.add(int(value))
        except (TypeError, ValueError):
            pass
    return result


def _bucket_output_complete(config, bucket_id):
    silver_bucket_path = _bucket_dir(output_path(config, "silver"), "transactions", bucket_id)
    gold_bucket_path = _bucket_dir(output_path(config, "gold"), "fact_transactions", bucket_id)
    return _delta_exists(silver_bucket_path) and _delta_exists(gold_bucket_path)


def _bucket_partition_bytes(config, bucket_id):
    transactions_path = Path(output_path(config, "bronze", "transactions"))
    candidates = [
        transactions_path / f"_processing_bucket={int(bucket_id)}",
        transactions_path / f"_processing_bucket={int(bucket_id):05d}",
    ]
    partition_path = next((path for path in candidates if path.exists()), None)
    if partition_path is None:
        return 0

    total = 0
    for file_path in partition_path.rglob("*"):
        if not file_path.is_file() or file_path.name.startswith("."):
            continue
        try:
            total += int(file_path.stat().st_size)
        except OSError:
            pass
    return total


def _estimate_bucket_work(config, manifest, bucket_count):
    previous_metrics = manifest.get("bucket_metrics") or {}
    source_total = METRICS["source_record_counts"].get("transactions_raw", 0)
    estimates = {}
    total_bytes = 0
    for bucket_id in range(int(bucket_count)):
        bucket_bytes = _bucket_partition_bytes(config, bucket_id)
        total_bytes += bucket_bytes
        prior = previous_metrics.get(str(bucket_id), {})
        estimates[str(bucket_id)] = {
            "estimated_bytes": bucket_bytes,
            "previous_source_count": prior.get("source_count"),
            "previous_duration_seconds": prior.get("duration_seconds"),
        }

    for bucket_id in range(int(bucket_count)):
        estimate = estimates[str(bucket_id)]
        if estimate["previous_source_count"] is not None:
            estimated_rows = int(estimate["previous_source_count"])
            row_source = "previous_bucket_metric"
        elif total_bytes > 0 and source_total:
            estimated_rows = int(round(source_total * estimate["estimated_bytes"] / total_bytes))
            row_source = "bronze_partition_byte_ratio"
        else:
            estimated_rows = None
            row_source = "unavailable"

        if estimate["previous_duration_seconds"] is not None:
            priority_weight = float(estimate["previous_duration_seconds"])
            priority_source = "previous_duration_seconds"
        elif estimated_rows is not None:
            priority_weight = float(estimated_rows)
            priority_source = "estimated_rows"
        else:
            priority_weight = float(estimate["estimated_bytes"])
            priority_source = "estimated_bytes"

        estimate["estimated_rows"] = estimated_rows
        estimate["estimated_rows_source"] = row_source
        estimate["priority_weight"] = priority_weight
        estimate["priority_source"] = priority_source

    return estimates


def _scheduled_bucket_order(config, manifest, preflight, bucket_estimates):
    bucket_count = int(preflight["bucket_count"])
    strategy = str(preflight.get("schedule_strategy") or DEFAULT_SCHEDULE_STRATEGY).strip().lower()
    completed_with_outputs = {
        bucket_id
        for bucket_id in _int_set(manifest.get("completed_buckets"))
        if 0 <= bucket_id < bucket_count and _bucket_output_complete(config, bucket_id)
    }

    def by_weight(bucket_id):
        estimate = bucket_estimates.get(str(bucket_id), {})
        return (-float(estimate.get("priority_weight") or 0), int(bucket_id))

    if strategy == "sequential":
        order = [bucket_id for bucket_id in range(bucket_count) if bucket_id not in completed_with_outputs]
        priority_groups = [{"name": "incomplete_sequential", "buckets": order}]
    else:
        active = manifest.get("active_bucket")
        try:
            active = int(active) if active is not None else None
        except (TypeError, ValueError):
            active = None
        active_group = []
        if active is not None and 0 <= active < bucket_count and active not in completed_with_outputs:
            active_group = [active]

        failed_group = sorted(
            [
                bucket_id
                for bucket_id in _int_set(manifest.get("failed_buckets"))
                if 0 <= bucket_id < bucket_count and bucket_id not in completed_with_outputs and bucket_id not in active_group
            ],
            key=by_weight,
        )
        remaining_group = sorted(
            [
                bucket_id
                for bucket_id in range(bucket_count)
                if bucket_id not in completed_with_outputs
                and bucket_id not in active_group
                and bucket_id not in failed_group
            ],
            key=by_weight,
        )
        order = active_group + failed_group + remaining_group
        priority_groups = [
            {"name": "resume_active_bucket", "buckets": active_group},
            {"name": "retry_failed_largest_first", "buckets": failed_group},
            {"name": "incomplete_largest_first", "buckets": remaining_group},
        ]

    return {
        "strategy": strategy,
        "bucket_order": order,
        "skipped_completed_buckets": sorted(completed_with_outputs),
        "priority_groups": priority_groups,
        "bucket_estimate_count": len(bucket_estimates),
        "created_at": RUN_TIMESTAMP,
    }


def _union_delta_paths(spark, paths):
    frames = [read_delta(spark, path) for path in paths]
    if not frames:
        return None
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame, allowMissingColumns=True)
    return result


def _sum_bucket_metrics(manifest, bucket_count):
    totals = {}
    for bucket_id in range(int(bucket_count)):
        metrics = (manifest.get("bucket_metrics") or {}).get(str(bucket_id), {})
        for key, value in metrics.items():
            if isinstance(value, int):
                totals[key] = totals.get(key, 0) + value
    return totals


def run_adaptive_ingestion(preflight):
    config = load_config()
    spark = spark_session(config)
    input_config = config["input"]
    ingestion_ts = _run_timestamp_col()
    bucket_count = int(preflight["bucket_count"])

    accounts = _read_csv_raw(spark, input_config["accounts_path"]).withColumn("ingestion_timestamp", ingestion_ts)
    customers = _read_csv_raw(spark, input_config["customers_path"]).withColumn("ingestion_timestamp", ingestion_ts)
    transactions = add_processing_bucket(raw_transaction_frame(spark, input_config["transactions_path"], ingestion_ts), bucket_count)

    accounts_metrics = write_delta(accounts, output_path(config, "bronze", "accounts"))
    customers_metrics = write_delta(customers, output_path(config, "bronze", "customers"))
    transactions_metrics = write_delta(
        transactions,
        output_path(config, "bronze", "transactions"),
        partition_by=["_processing_bucket"],
    )

    set_source_count("accounts_raw", metric_int(accounts_metrics, "numOutputRows"))
    set_source_count("customers_raw", metric_int(customers_metrics, "numOutputRows"))
    set_source_count("transactions_raw", metric_int(transactions_metrics, "numOutputRows"))
    return {
        "accounts": accounts_metrics,
        "customers": customers_metrics,
        "transactions": transactions_metrics,
    }


def _transform_dimensions(config):
    spark = spark_session(config)
    bronze_root = output_path(config, "bronze")
    silver_root = output_path(config, "silver")
    gold_root = output_path(config, "gold")

    customers_b = read_delta(spark, f"{bronze_root}/customers")
    accounts_b = read_delta(spark, f"{bronze_root}/accounts")

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
        invalid_gender_count=_count_if(invalid_domain(F.col("gender"), GENDERS)),
        invalid_customer_province_count=_count_if(invalid_domain(F.col("province"), SA_PROVINCES)),
        invalid_kyc_status_count=_count_if(invalid_domain(F.col("kyc_status"), KYC_STATUSES)),
        gender_distinct_count=approx_distinct_values(F.col("gender")),
        gender_unknown_distinct_count=approx_distinct_unknown_values(F.col("gender"), GENDERS),
        customer_province_distinct_count=approx_distinct_values(F.col("province")),
        customer_province_unknown_distinct_count=approx_distinct_unknown_values(F.col("province"), SA_PROVINCES),
        kyc_status_distinct_count=approx_distinct_values(F.col("kyc_status")),
        kyc_status_unknown_distinct_count=approx_distinct_unknown_values(F.col("kyc_status"), KYC_STATUSES),
        risk_score_outside_range_count=_count_if(
            F.col("risk_score").isNotNull() & ((F.col("risk_score") < 1) | (F.col("risk_score") > 10))
        ),
        risk_score_quantiles=F.expr("percentile_approx(risk_score, array(0.5, 0.95, 0.99), 100)"),
    )
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
        invalid_account_type_count=_count_if(invalid_domain(F.col("account_type"), ACCOUNT_TYPES)),
        invalid_account_status_count=_count_if(invalid_domain(F.col("account_status"), ACCOUNT_STATUSES)),
        invalid_product_tier_count=_count_if(invalid_domain(F.col("product_tier"), PRODUCT_TIERS)),
        invalid_digital_channel_count=_count_if(invalid_domain(F.col("digital_channel"), DIGITAL_CHANNELS)),
        account_type_distinct_count=approx_distinct_values(F.col("account_type")),
        account_type_unknown_distinct_count=approx_distinct_unknown_values(F.col("account_type"), ACCOUNT_TYPES),
        account_status_distinct_count=approx_distinct_values(F.col("account_status")),
        account_status_unknown_distinct_count=approx_distinct_unknown_values(F.col("account_status"), ACCOUNT_STATUSES),
        product_tier_distinct_count=approx_distinct_values(F.col("product_tier")),
        product_tier_unknown_distinct_count=approx_distinct_unknown_values(F.col("product_tier"), PRODUCT_TIERS),
        digital_channel_distinct_count=approx_distinct_values(F.col("digital_channel")),
        digital_channel_unknown_distinct_count=approx_distinct_unknown_values(F.col("digital_channel"), DIGITAL_CHANNELS),
        negative_current_balance_count=_count_if(F.col("current_balance") < 0),
        negative_credit_limit_count=_count_if(F.col("credit_limit") < 0),
        current_balance_quantiles=F.expr("percentile_approx(current_balance_double, array(0.5, 0.95, 0.99), 100)"),
    )
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
    ).persist(StorageLevel.MEMORY_AND_DISK)

    customer_write_metrics = write_delta(customers, f"{silver_root}/customers")
    account_write_metrics = write_delta(accounts_valid, f"{silver_root}/accounts")
    if account_metrics["null_account_count"]:
        write_delta(account_quarantine, f"{silver_root}/quarantine_accounts")

    dim_customers = _with_surrogate_key(customers, "customer_id", "customer_sk").select(
        "customer_sk",
        "customer_id",
        "gender",
        "province",
        "income_band",
        "segment",
        "risk_score",
        "kyc_status",
        _age_band(F.col("dob")).alias("age_band"),
    )
    dim_accounts = _with_surrogate_key(accounts_valid, "account_id", "account_sk").select(
        "account_sk",
        "account_id",
        F.col("customer_ref").alias("customer_id"),
        "account_type",
        "account_status",
        "open_date",
        "product_tier",
        "digital_channel",
        "credit_limit",
        "current_balance",
        "last_activity_date",
    )
    customer_gold_metrics = write_delta(dim_customers, f"{gold_root}/dim_customers")
    account_gold_metrics = write_delta(dim_accounts, f"{gold_root}/dim_accounts")
    set_gold_count("dim_customers", metric_int(customer_gold_metrics, "numOutputRows"))
    set_gold_count("dim_accounts", metric_int(account_gold_metrics, "numOutputRows"))

    account_ids = accounts_valid.select("account_id").withColumn("_account_exists", F.lit(True)).persist(StorageLevel.DISK_ONLY)
    account_customer = dim_accounts.join(
        F.broadcast(dim_customers.select("customer_id", "customer_sk", F.col("province").alias("customer_province"))),
        "customer_id",
        "inner",
    ).persist(StorageLevel.DISK_ONLY)

    return {
        "customers": customers,
        "accounts_valid": accounts_valid,
        "account_ids": account_ids,
        "account_customer": account_customer,
        "customer_metrics": customer_metrics,
        "account_metrics": account_metrics,
        "account_write_metrics": account_write_metrics,
        "customer_write_metrics": customer_write_metrics,
    }


def _prepare_transactions(transactions_b):
    transactions = add_missing_columns(transactions_b, {"merchant_subcategory": T.StringType()})
    return (
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


def _transform_transaction_bucket(config, bucket_id, dimensions):
    spark = spark_session(config)
    bronze_root = output_path(config, "bronze")
    silver_root = output_path(config, "silver")
    gold_root = output_path(config, "gold")

    transactions_b = read_delta(spark, f"{bronze_root}/transactions").where(F.col("_processing_bucket") == int(bucket_id))
    transactions = _prepare_transactions(transactions_b)
    transaction_metrics = _aggregate_metrics(
        transactions,
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
        date_transaction_count=_count_if(F.col("transaction_date_variant")),
        transaction_date_parse_failed_count=_count_if(~is_blank(F.col("transaction_date")) & F.col("transaction_date_parsed").isNull()),
        currency_count=_count_if(F.col("currency_variant")),
        invalid_currency_count=_count_if(F.col("currency_clean").isNotNull() & (F.col("currency_clean") != "ZAR")),
        invalid_transaction_type_count=_count_if(invalid_domain(F.col("transaction_type"), TRANSACTION_TYPES)),
        invalid_channel_count=_count_if(invalid_domain(F.col("channel"), TRANSACTION_CHANNELS)),
        invalid_transaction_province_count=_count_if(invalid_domain(F.col("province"), SA_PROVINCES)),
    )
    transaction_id_counts = transactions.groupBy("transaction_id").agg(F.count(F.lit(1)).cast("long").alias("_group_count"))
    duplicate_count = _aggregate_metrics(
        transaction_id_counts,
        duplicate_count=F.sum(F.col("_group_count") - F.lit(1)).cast("long"),
    )["duplicate_count"]
    transactions_deduped = _dedupe_transactions_grouped(transactions).select("_picked.*") if duplicate_count else transactions

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
    transactions_joined = transactions_deduped.join(F.broadcast(dimensions["account_ids"]), "account_id", "left")
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
    transactions_valid = transactions_joined.where(valid_transaction_condition).drop("_account_exists")
    transactions_silver = (
        transactions_valid.withColumn(
            "dq_flag",
            F.when(F.col("amount_type_mismatch"), F.lit("TYPE_MISMATCH"))
            .when(F.col("transaction_date_variant"), F.lit("DATE_FORMAT"))
            .when(F.col("currency_variant"), F.lit("CURRENCY_VARIANT"))
            .otherwise(F.lit(None).cast("string")),
        )
        .select(
            "transaction_id",
            "account_id",
            F.col("transaction_date_parsed").alias("transaction_date"),
            "transaction_timestamp",
            "transaction_type",
            "merchant_category",
            "merchant_subcategory",
            F.col("amount_decimal").alias("amount"),
            F.col("currency_clean").alias("currency"),
            "channel",
            "province",
            "dq_flag",
            "ingestion_timestamp",
        )
    )
    fact_base = transactions_silver.join(
        F.broadcast(dimensions["account_customer"].select("account_id", "account_sk", "customer_sk", "customer_province")),
        "account_id",
        "inner",
    )
    fact_transactions = _with_surrogate_key(fact_base, "transaction_id", "transaction_sk").select(
        "transaction_sk",
        "transaction_id",
        "account_sk",
        "customer_sk",
        "transaction_date",
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        "merchant_subcategory",
        "amount",
        "currency",
        "channel",
        F.coalesce(F.col("customer_province"), F.col("province")).alias("province"),
        "dq_flag",
        "ingestion_timestamp",
    )

    silver_bucket_path = _bucket_dir(silver_root, "transactions", bucket_id)
    gold_bucket_path = _bucket_dir(gold_root, "fact_transactions", bucket_id)
    silver_metrics = write_delta(transactions_silver, silver_bucket_path)
    gold_metrics = write_delta(fact_transactions, gold_bucket_path)
    if not _delta_exists(silver_bucket_path) or not _delta_exists(gold_bucket_path):
        raise RuntimeError(f"bucket {bucket_id} write completed without expected Delta log")

    quarantine_transactions = None
    if duplicate_count:
        duplicate_ids = transaction_id_counts.where(F.col("_group_count") > 1).select("transaction_id")
        quarantine_transactions = _quarantined_duplicates(transactions, duplicate_ids).select("raw_json")
    if deduped_metrics["orphan_count"]:
        orphan_rows = transactions_joined.where(orphaned_condition).drop("_account_exists").select("raw_json")
        quarantine_transactions = orphan_rows if quarantine_transactions is None else quarantine_transactions.unionByName(orphan_rows, allowMissingColumns=True)
    if deduped_metrics["bad_required_count"]:
        bad_required_rows = transactions_joined.where(required_missing).drop("_account_exists").select("raw_json")
        quarantine_transactions = bad_required_rows if quarantine_transactions is None else quarantine_transactions.unionByName(bad_required_rows, allowMissingColumns=True)
    if quarantine_transactions is not None:
        write_delta(quarantine_transactions, _bucket_dir(silver_root, "quarantine_transactions", bucket_id))

    result = {
        "source_count": transaction_metrics["source_count"],
        "duplicate_count": duplicate_count,
        "orphan_count": deduped_metrics["orphan_count"],
        "bad_required_count": deduped_metrics["bad_required_count"],
        "type_count": transaction_metrics["type_count"],
        "type_output_count": deduped_metrics["type_output_count"],
        "date_transaction_count": transaction_metrics["date_transaction_count"],
        "currency_count": transaction_metrics["currency_count"],
        "currency_output_count": deduped_metrics["currency_output_count"],
        "amount_cast_failed_count": transaction_metrics["amount_cast_failed_count"],
        "null_transaction_id": transaction_metrics["null_transaction_id"],
        "null_transaction_account_id": transaction_metrics["null_transaction_account_id"],
        "null_transaction_date": transaction_metrics["null_transaction_date"],
        "null_transaction_time": transaction_metrics["null_transaction_time"],
        "null_transaction_amount": transaction_metrics["null_transaction_amount"],
        "negative_amount_count": transaction_metrics["negative_amount_count"],
        "zero_amount_count": transaction_metrics["zero_amount_count"],
        "extreme_amount_count": transaction_metrics["extreme_amount_count"],
        "transaction_date_parse_failed_count": transaction_metrics["transaction_date_parse_failed_count"],
        "invalid_currency_count": transaction_metrics["invalid_currency_count"],
        "invalid_transaction_type_count": transaction_metrics["invalid_transaction_type_count"],
        "invalid_channel_count": transaction_metrics["invalid_channel_count"],
        "invalid_transaction_province_count": transaction_metrics["invalid_transaction_province_count"],
        "silver_output_rows": metric_int(silver_metrics, "numOutputRows"),
        "gold_output_rows": metric_int(gold_metrics, "numOutputRows"),
    }
    return result


def _add_aggregated_issues(config, dimensions, transaction_totals):
    rules = load_dq_rules()
    source_transactions = METRICS["source_record_counts"].get("transactions_raw", transaction_totals.get("source_count", 0))
    source_accounts = METRICS["source_record_counts"].get("accounts_raw", dimensions["account_metrics"]["source_count"])
    source_customers = METRICS["source_record_counts"].get("customers_raw", dimensions["customer_metrics"]["source_count"])
    source_all = source_transactions + source_accounts + source_customers
    account_metrics = dimensions["account_metrics"]
    customer_metrics = dimensions["customer_metrics"]
    add_issue("DUPLICATE_DEDUPED", transaction_totals.get("duplicate_count", 0), source_transactions, _action("DUPLICATE_DEDUPED", rules), source_transactions - transaction_totals.get("duplicate_count", 0))
    add_issue("ORPHANED_ACCOUNT", transaction_totals.get("orphan_count", 0), source_transactions, _action("ORPHANED_ACCOUNT", rules), 0)
    add_issue("TYPE_MISMATCH", transaction_totals.get("type_count", 0), source_transactions, _action("TYPE_MISMATCH", rules), transaction_totals.get("type_output_count", 0))
    add_issue(
        "DATE_FORMAT",
        transaction_totals.get("date_transaction_count", 0) + account_metrics["date_account_count"] + customer_metrics["date_customer_count"],
        source_all,
        _action("DATE_FORMAT", rules),
        transaction_totals.get("date_transaction_count", 0) + account_metrics["date_account_count"] + customer_metrics["date_customer_count"],
    )
    add_issue("CURRENCY_VARIANT", transaction_totals.get("currency_count", 0), source_transactions, _action("CURRENCY_VARIANT", rules), transaction_totals.get("currency_output_count", 0))
    add_issue("NULL_REQUIRED", account_metrics["null_account_count"], source_accounts, _action("NULL_REQUIRED", rules), 0)


def _set_adaptive_light_profile(dimensions, transaction_totals):
    source_transactions = METRICS["source_record_counts"].get("transactions_raw", transaction_totals.get("source_count", 0))
    source_accounts = METRICS["source_record_counts"].get("accounts_raw", dimensions["account_metrics"]["source_count"])
    source_customers = METRICS["source_record_counts"].get("customers_raw", dimensions["customer_metrics"]["source_count"])
    transaction_metrics = dict(transaction_totals)
    transaction_metrics.setdefault("amount_quantiles", [])
    _set_light_raw_profile(
        source_transactions,
        source_accounts,
        source_customers,
        transaction_metrics,
        transaction_totals.get("duplicate_count", 0),
        {"orphan_count": transaction_totals.get("orphan_count", 0)},
        dimensions["account_metrics"],
        dimensions["customer_metrics"],
        metric_int(dimensions["account_write_metrics"], "numOutputRows"),
    )
    set_raw_profile_section(
        "adaptive_chunking",
        {
            "enabled": True,
            "transaction_bucket_count": len(transaction_totals.get("completed_buckets", [])),
        },
    )


def _active_delta_files(table_path):
    table_path = Path(table_path)
    active = {}
    for log_file in sorted((table_path / "_delta_log").glob("*.json")):
        with open(log_file, "r", encoding="utf-8") as handle:
            for line in handle:
                action = json.loads(line)
                if "add" in action:
                    active[action["add"]["path"]] = True
                elif "remove" in action:
                    active.pop(action["remove"]["path"], None)
    return [str(table_path / path) for path in sorted(active)]


def _sql_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def _validate_gold_with_duckdb(gold_root):
    import duckdb

    con = duckdb.connect()
    for table in ("fact_transactions", "dim_accounts", "dim_customers"):
        files = _active_delta_files(Path(gold_root) / table)
        file_list = ", ".join(_sql_quote(path) for path in files)
        con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM parquet_scan([{file_list}])")
    type_rows = con.execute("SELECT transaction_type, COUNT(*) FROM fact_transactions GROUP BY transaction_type").fetchall()
    unlinked = con.execute(
        """
        SELECT COUNT(*)
        FROM dim_accounts a
        LEFT JOIN dim_customers c ON a.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """
    ).fetchone()[0]
    province_rows = con.execute(
        """
        SELECT COUNT(*)
        FROM (
          SELECT c.province, COUNT(DISTINCT a.account_id)
          FROM dim_accounts a
          JOIN dim_customers c ON a.customer_id = c.customer_id
          GROUP BY c.province
        )
        """
    ).fetchone()[0]
    return {
        "transaction_type_rows": len(type_rows),
        "unlinked_accounts": int(unlinked),
        "province_rows": int(province_rows),
        "passed": len(type_rows) == 4 and int(unlinked) == 0 and int(province_rows) == 9,
    }


def run_adaptive_pipeline(preflight):
    config = load_config()
    spark = spark_session(config)
    bucket_count = int(preflight["bucket_count"])
    manifest = _load_manifest(preflight)
    _write_manifest(preflight, manifest)
    if manifest.get("finalized") and _finalized_outputs_exist(config, manifest):
        result = dict(manifest)
        result["_resume_action"] = "already_finalized"
        return result

    run_adaptive_ingestion(preflight)
    bucket_estimates = _estimate_bucket_work(config, manifest, bucket_count)
    schedule_plan = _scheduled_bucket_order(config, manifest, preflight, bucket_estimates)
    manifest["bucket_estimates"] = bucket_estimates
    manifest["schedule_plan"] = schedule_plan
    _write_manifest(preflight, manifest)

    dimensions = _transform_dimensions(config)
    try:
        completed = set(int(bucket) for bucket in manifest.get("completed_buckets", []))
        for bucket_id in schedule_plan["bucket_order"]:
            silver_bucket_path = _bucket_dir(output_path(config, "silver"), "transactions", bucket_id)
            gold_bucket_path = _bucket_dir(output_path(config, "gold"), "fact_transactions", bucket_id)
            if bucket_id in completed and _delta_exists(silver_bucket_path) and _delta_exists(gold_bucket_path):
                continue

            manifest["active_bucket"] = bucket_id
            manifest["finalized"] = False
            manifest["finalization_status"] = "not_started"
            _write_manifest(preflight, manifest)
            started_at = time.time()
            try:
                bucket_metrics = _transform_transaction_bucket(config, bucket_id, dimensions)
                bucket_metrics["duration_seconds"] = round(time.time() - started_at, 3)
                manifest.setdefault("bucket_metrics", {})[str(bucket_id)] = bucket_metrics
                completed.add(bucket_id)
                manifest["completed_buckets"] = sorted(completed)
                manifest["failed_buckets"] = [bucket for bucket in manifest.get("failed_buckets", []) if bucket != bucket_id]
                manifest["active_bucket"] = None
                _write_manifest(preflight, manifest)
            except Exception:
                failed = set(int(bucket) for bucket in manifest.get("failed_buckets", []))
                failed.add(bucket_id)
                manifest["failed_buckets"] = sorted(failed)
                manifest["active_bucket"] = bucket_id
                _write_manifest(preflight, manifest)
                raise

        manifest["finalization_status"] = "running"
        _write_manifest(preflight, manifest)
        silver_paths = [_bucket_dir(output_path(config, "silver"), "transactions", bucket_id) for bucket_id in range(bucket_count)]
        gold_paths = [_bucket_dir(output_path(config, "gold"), "fact_transactions", bucket_id) for bucket_id in range(bucket_count)]
        silver_transactions = _union_delta_paths(spark, silver_paths)
        fact_transactions = _union_delta_paths(spark, gold_paths)
        silver_metrics = write_delta(silver_transactions, f"{output_path(config, 'silver')}/transactions")
        fact_metrics = write_delta(fact_transactions, f"{output_path(config, 'gold')}/fact_transactions")
        set_gold_count("fact_transactions", metric_int(fact_metrics, "numOutputRows"))

        transaction_totals = _sum_bucket_metrics(manifest, bucket_count)
        transaction_totals["completed_buckets"] = manifest["completed_buckets"]
        _add_aggregated_issues(config, dimensions, transaction_totals)
        _set_adaptive_light_profile(dimensions, transaction_totals)
        _write_report_if_needed(config, infer_stage(config))

        validation = _validate_gold_with_duckdb(output_path(config, "gold"))
        if not validation["passed"]:
            raise RuntimeError(f"adaptive Gold validation failed: {validation}")
        manifest["finalization_status"] = "complete"
        manifest["finalized"] = True
        manifest["final_metrics"] = {
            "silver_transactions": silver_metrics,
            "fact_transactions": fact_metrics,
            "duckdb_validation": validation,
        }
        _write_manifest(preflight, manifest)
        return manifest
    finally:
        dimensions["account_ids"].unpersist()
        dimensions["account_customer"].unpersist()
        dimensions["accounts_valid"].unpersist()
