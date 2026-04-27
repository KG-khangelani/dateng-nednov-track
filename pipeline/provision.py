from pyspark.sql import functions as F

from pipeline.common import (
    RUN_TIMESTAMP,
    execution_seconds,
    infer_stage,
    load_config,
    output_path,
    read_delta,
    spark_session,
    write_delta,
    write_json_report,
)
from pipeline.metrics import METRICS, issue_list, set_gold_count


def _with_surrogate_key(df, natural_key, sk_name):
    return df.withColumn(
        sk_name,
        F.pmod(F.xxhash64(F.col(natural_key)), F.lit(9223372036854775807)).cast("bigint"),
    )


def _age_band(dob_col):
    age = F.floor(F.months_between(F.current_date(), dob_col) / F.lit(12))
    return (
        F.when(age >= 65, F.lit("65+"))
        .when(age >= 56, F.lit("56-65"))
        .when(age >= 46, F.lit("46-55"))
        .when(age >= 36, F.lit("36-45"))
        .when(age >= 26, F.lit("26-35"))
        .when(age >= 18, F.lit("18-25"))
        .otherwise(F.lit("18-25"))
    )


def _write_report_if_needed(config, stage):
    if stage == "1":
        return
    report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": RUN_TIMESTAMP,
        "stage": stage,
        "source_record_counts": {
            "accounts_raw": METRICS["source_record_counts"].get("accounts_raw", 0),
            "transactions_raw": METRICS["source_record_counts"].get("transactions_raw", 0),
            "customers_raw": METRICS["source_record_counts"].get("customers_raw", 0),
        },
        "dq_issues": issue_list(),
        "gold_layer_record_counts": {
            "fact_transactions": METRICS["gold_layer_record_counts"].get("fact_transactions", 0),
            "dim_accounts": METRICS["gold_layer_record_counts"].get("dim_accounts", 0),
            "dim_customers": METRICS["gold_layer_record_counts"].get("dim_customers", 0),
        },
        "execution_duration_seconds": execution_seconds(),
    }
    write_json_report(config, report)


def run_provisioning():
    config = load_config()
    stage = infer_stage(config)
    spark = spark_session(config)
    silver_root = output_path(config, "silver")
    gold_root = output_path(config, "gold")

    customers = read_delta(spark, f"{silver_root}/customers")
    accounts = read_delta(spark, f"{silver_root}/accounts")
    transactions = read_delta(spark, f"{silver_root}/transactions")

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

    dim_accounts = _with_surrogate_key(accounts, "account_id", "account_sk").select(
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

    account_customer = dim_accounts.join(
        dim_customers.select("customer_id", "customer_sk", F.col("province").alias("customer_province")),
        "customer_id",
        "inner",
    )

    fact_base = transactions.join(
        account_customer.select("account_id", "account_sk", "customer_sk", "customer_province"),
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

    write_delta(dim_customers, f"{gold_root}/dim_customers")
    write_delta(dim_accounts, f"{gold_root}/dim_accounts")
    write_delta(fact_transactions, f"{gold_root}/fact_transactions")

    set_gold_count("dim_customers", dim_customers.count())
    set_gold_count("dim_accounts", dim_accounts.count())
    set_gold_count("fact_transactions", fact_transactions.count())
    _write_report_if_needed(config, stage)
