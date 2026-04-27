from pyspark.sql import functions as F

from pipeline.common import RUN_TIMESTAMP, load_config, output_path, raw_transaction_frame, spark_session, write_delta
from pipeline.metrics import set_source_count


def _run_timestamp_col():
    return F.to_timestamp(F.lit(RUN_TIMESTAMP.replace("Z", "")))


def _read_csv_raw(spark, path):
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(path)
    )


def run_ingestion():
    config = load_config()
    spark = spark_session(config)
    input_config = config["input"]
    ingestion_ts = _run_timestamp_col()

    accounts = _read_csv_raw(spark, input_config["accounts_path"]).withColumn("ingestion_timestamp", ingestion_ts)
    customers = _read_csv_raw(spark, input_config["customers_path"]).withColumn("ingestion_timestamp", ingestion_ts)
    transactions = raw_transaction_frame(spark, input_config["transactions_path"], ingestion_ts)

    set_source_count("accounts_raw", accounts.count())
    set_source_count("customers_raw", customers.count())
    set_source_count("transactions_raw", transactions.count())

    write_delta(accounts, output_path(config, "bronze", "accounts"))
    write_delta(customers, output_path(config, "bronze", "customers"))
    write_delta(transactions, output_path(config, "bronze", "transactions"))

