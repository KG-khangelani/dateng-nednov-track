import time
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.common import (
    normalise_currency,
    parse_timestamp,
    raw_transaction_frame,
    read_delta,
    spark_session,
    write_delta,
)
from pipeline.common import load_config


CURRENT_BALANCES_SCHEMA = T.StructType(
    [
        T.StructField("account_id", T.StringType(), False),
        T.StructField("current_balance", T.DecimalType(18, 2), False),
        T.StructField("last_transaction_timestamp", T.TimestampType(), False),
        T.StructField("updated_at", T.TimestampType(), False),
    ]
)

RECENT_TRANSACTIONS_SCHEMA = T.StructType(
    [
        T.StructField("account_id", T.StringType(), False),
        T.StructField("transaction_id", T.StringType(), False),
        T.StructField("transaction_timestamp", T.TimestampType(), False),
        T.StructField("amount", T.DecimalType(18, 2), False),
        T.StructField("transaction_type", T.StringType(), False),
        T.StructField("channel", T.StringType(), True),
        T.StructField("updated_at", T.TimestampType(), False),
    ]
)


def _table_exists(path):
    return (Path(path) / "_delta_log").exists()


def _read_or_empty(spark, path, schema):
    if _table_exists(path):
        return read_delta(spark, path)
    return spark.createDataFrame([], schema)


def _updated_at_col():
    return F.expr("transaction_timestamp + INTERVAL 60 seconds")


def _normalise_stream_events(spark, file_path, dim_accounts):
    raw = raw_transaction_frame(spark, str(file_path))
    parsed = (
        raw.withColumn("transaction_timestamp", parse_timestamp(F.col("transaction_date"), F.col("transaction_time")))
        .withColumn("amount", F.col("amount").cast(T.DecimalType(18, 2)))
        .withColumn("currency", normalise_currency(F.col("currency")))
        .where(
            F.col("transaction_id").isNotNull()
            & F.col("account_id").isNotNull()
            & F.col("transaction_timestamp").isNotNull()
            & F.col("amount").isNotNull()
            & F.col("transaction_type").isin("DEBIT", "CREDIT", "FEE", "REVERSAL")
            & (F.col("currency") == "ZAR")
        )
    )
    return parsed.join(dim_accounts.select("account_id"), "account_id", "inner")


def _update_current_balances(spark, events, current_path, dim_accounts):
    existing = _read_or_empty(spark, current_path, CURRENT_BALANCES_SCHEMA)
    if existing.count() == 0:
        existing = dim_accounts.select(
            "account_id",
            "current_balance",
            F.to_timestamp(F.lit("1970-01-01 00:00:00")).alias("last_transaction_timestamp"),
            F.to_timestamp(F.lit("1970-01-01 00:00:00")).alias("updated_at"),
        )

    deltas = (
        events.withColumn(
            "balance_delta",
            F.when(F.col("transaction_type").isin("CREDIT", "REVERSAL"), F.col("amount")).otherwise(-F.col("amount")),
        )
        .groupBy("account_id")
        .agg(
            F.sum("balance_delta").cast(T.DecimalType(18, 2)).alias("balance_delta"),
            F.max("transaction_timestamp").alias("delta_last_transaction_timestamp"),
            F.max(_updated_at_col()).alias("delta_updated_at"),
        )
    )
    updated = (
        existing.join(deltas, "account_id", "left")
        .select(
            "account_id",
            (F.col("current_balance") + F.coalesce(F.col("balance_delta"), F.lit(0).cast(T.DecimalType(18, 2)))).cast(T.DecimalType(18, 2)).alias("current_balance"),
            F.coalesce(F.col("delta_last_transaction_timestamp"), F.col("last_transaction_timestamp")).alias("last_transaction_timestamp"),
            F.coalesce(F.col("delta_updated_at"), F.col("updated_at")).alias("updated_at"),
        )
    )
    write_delta(updated.localCheckpoint(eager=True), current_path)


def _update_recent_transactions(spark, events, recent_path):
    existing = _read_or_empty(spark, recent_path, RECENT_TRANSACTIONS_SCHEMA)
    batch = events.select(
        "account_id",
        "transaction_id",
        "transaction_timestamp",
        "amount",
        "transaction_type",
        "channel",
        _updated_at_col().alias("updated_at"),
    )
    dedupe_window = Window.partitionBy("account_id", "transaction_id").orderBy(F.col("transaction_timestamp").desc())
    rank_window = Window.partitionBy("account_id").orderBy(F.col("transaction_timestamp").desc(), F.col("transaction_id"))
    retained = (
        existing.unionByName(batch)
        .withColumn("_dedupe_rn", F.row_number().over(dedupe_window))
        .where(F.col("_dedupe_rn") == 1)
        .drop("_dedupe_rn")
        .withColumn("_recent_rn", F.row_number().over(rank_window))
        .where(F.col("_recent_rn") <= 50)
        .drop("_recent_rn")
    )
    write_delta(retained.localCheckpoint(eager=True), recent_path)


def run_stream_ingestion():
    config = load_config()
    spark = spark_session(config)
    stream_config = config.get("streaming", {})
    stream_dir = Path(stream_config.get("stream_input_path", "/data/stream"))
    stream_gold_root = stream_config.get("stream_gold_path", "/data/output/stream_gold")
    poll_interval = int(stream_config.get("poll_interval_seconds", 10))
    quiesce_timeout = int(stream_config.get("quiesce_timeout_seconds", 60))

    if not stream_dir.exists():
        return

    current_path = f"{stream_gold_root}/current_balances"
    recent_path = f"{stream_gold_root}/recent_transactions"
    dim_accounts = read_delta(spark, f"{config['output']['gold_path']}/dim_accounts")
    processed = set()
    last_new_file = time.time()

    while True:
        files = sorted(stream_dir.glob("stream_*.jsonl"))
        new_files = [path for path in files if path.name not in processed]
        if not new_files and time.time() - last_new_file >= quiesce_timeout:
            break

        for file_path in new_files:
            events = _normalise_stream_events(spark, file_path, dim_accounts)
            events = events.cache()
            if events.count() > 0:
                _update_current_balances(spark, events, current_path, dim_accounts)
                _update_recent_transactions(spark, events, recent_path)
            events.unpersist()
            processed.add(file_path.name)
            last_new_file = time.time()

        if not new_files:
            time.sleep(poll_interval)
