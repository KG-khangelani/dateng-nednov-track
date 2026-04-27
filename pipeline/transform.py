from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.common import (
    add_missing_columns,
    currency_variant_flag,
    date_variant_flag,
    load_config,
    load_dq_rules,
    normalise_currency,
    output_path,
    parse_date,
    parse_timestamp,
    read_delta,
    spark_session,
    write_delta,
)
from pipeline.metrics import METRICS, add_issue


DEFAULT_ACTIONS = {
    "DUPLICATE_DEDUPED": "DEDUPLICATED_KEEP_FIRST",
    "ORPHANED_ACCOUNT": "QUARANTINED",
    "TYPE_MISMATCH": "CAST_TO_DECIMAL",
    "DATE_FORMAT": "NORMALISED_DATE",
    "CURRENCY_VARIANT": "NORMALISED_CURRENCY",
    "NULL_REQUIRED": "EXCLUDED_NULL_PK",
}


def _action(issue_type):
    rule = (load_dq_rules().get("rules") or {}).get(issue_type, {})
    return rule.get("handling_action", DEFAULT_ACTIONS[issue_type])


def _dedupe(df, key, order_cols):
    window = Window.partitionBy(key).orderBy(*[F.col(c).asc_nulls_last() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def _issue_count(df, predicate):
    return df.where(predicate).count()


def run_transformation():
    config = load_config()
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
    date_customer_count = _issue_count(customers, F.col("dob_date_variant"))
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
        .withColumn(
            "date_variant",
            date_variant_flag(F.col("open_date"))
            | (F.col("last_activity_date").isNotNull() & date_variant_flag(F.col("last_activity_date"))),
        )
    )
    null_account_count = _issue_count(accounts, F.col("account_id").isNull() | (F.trim(F.col("account_id")) == ""))
    date_account_count = _issue_count(accounts, F.col("date_variant"))
    account_quarantine = accounts.where(F.col("account_id").isNull() | (F.trim(F.col("account_id")) == ""))
    accounts_valid = accounts.where(F.col("account_id").isNotNull() & (F.trim(F.col("account_id")) != ""))
    accounts_valid = _dedupe(accounts_valid, "account_id", ["account_id"])
    accounts_valid = accounts_valid.join(customers.select("customer_id"), accounts_valid.customer_ref == customers.customer_id, "inner")
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

    transactions = add_missing_columns(transactions_b, {"merchant_subcategory": T.StringType()})
    transactions = (
        transactions.withColumn("amount_decimal", F.col("amount").cast(T.DecimalType(18, 2)))
        .withColumn("transaction_date_parsed", parse_date(F.col("transaction_date")))
        .withColumn("transaction_timestamp", parse_timestamp(F.col("transaction_date"), F.col("transaction_time")))
        .withColumn("currency_clean", normalise_currency(F.col("currency")))
        .withColumn("province", F.col("location.province"))
        .withColumn("amount_type_mismatch", F.col("amount_was_quoted") & F.col("amount_decimal").isNotNull())
        .withColumn("amount_cast_failed", F.col("amount").isNotNull() & F.col("amount_decimal").isNull())
        .withColumn("transaction_date_variant", date_variant_flag(F.col("transaction_date")))
        .withColumn("currency_variant", currency_variant_flag(F.col("currency")))
    )

    source_transactions = METRICS["source_record_counts"].get("transactions_raw", transactions.count())
    source_accounts = METRICS["source_record_counts"].get("accounts_raw", accounts_b.count())
    source_all = source_transactions + source_accounts + METRICS["source_record_counts"].get("customers_raw", customers_b.count())

    duplicate_window = Window.partitionBy("transaction_id").orderBy(F.col("transaction_timestamp").asc_nulls_last(), F.col("raw_json"))
    transactions_ranked = transactions.withColumn("_rn", F.row_number().over(duplicate_window))
    duplicate_count = transactions_ranked.where(F.col("_rn") > 1).count()
    transactions_deduped = transactions_ranked.where(F.col("_rn") == 1).drop("_rn")

    orphaned = transactions_deduped.join(accounts_valid.select("account_id"), "account_id", "left_anti")
    orphan_count = orphaned.count()

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
    bad_required = transactions_deduped.where(required_missing)
    cleanable_transactions = transactions_deduped.where(~required_missing)
    transactions_valid = cleanable_transactions.join(accounts_valid.select("account_id"), "account_id", "inner")

    type_count = _issue_count(transactions, F.col("amount_type_mismatch") | F.col("amount_cast_failed"))
    date_transaction_count = _issue_count(transactions, F.col("transaction_date_variant"))
    currency_count = _issue_count(transactions, F.col("currency_variant"))

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

    add_issue("DUPLICATE_DEDUPED", duplicate_count, source_transactions, _action("DUPLICATE_DEDUPED"), transactions_deduped.count())
    add_issue("ORPHANED_ACCOUNT", orphan_count, source_transactions, _action("ORPHANED_ACCOUNT"), 0)
    add_issue("TYPE_MISMATCH", type_count, source_transactions, _action("TYPE_MISMATCH"), transactions_silver.where(F.col("dq_flag") == "TYPE_MISMATCH").count())
    add_issue("DATE_FORMAT", date_transaction_count + date_account_count + date_customer_count, source_all, _action("DATE_FORMAT"), date_transaction_count + date_account_count + date_customer_count)
    add_issue("CURRENCY_VARIANT", currency_count, source_transactions, _action("CURRENCY_VARIANT"), transactions_silver.where(F.col("dq_flag") == "CURRENCY_VARIANT").count())
    add_issue("NULL_REQUIRED", null_account_count, source_accounts, _action("NULL_REQUIRED"), 0)

    write_delta(customers, f"{silver_root}/customers")
    write_delta(accounts_valid, f"{silver_root}/accounts")
    write_delta(transactions_silver, f"{silver_root}/transactions")

    if duplicate_count or orphan_count or null_account_count or bad_required.count():
        write_delta(
            transactions_ranked.where(F.col("_rn") > 1).select("raw_json").unionByName(orphaned.select("raw_json"), allowMissingColumns=True).unionByName(bad_required.select("raw_json"), allowMissingColumns=True),
            f"{silver_root}/quarantine_transactions",
        )
        write_delta(account_quarantine, f"{silver_root}/quarantine_accounts")
