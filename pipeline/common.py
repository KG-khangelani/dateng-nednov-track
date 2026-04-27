import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from delta import configure_spark_with_delta_pip
import pyspark
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


RUN_STARTED_AT = time.time()
RUN_TIMESTAMP = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = "/data/config/pipeline_config.yaml"
DEFAULT_DQ_RULES = "/data/config/dq_rules.yaml"


TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType(), True),
        T.StructField("account_id", T.StringType(), True),
        T.StructField("transaction_date", T.StringType(), True),
        T.StructField("transaction_time", T.StringType(), True),
        T.StructField("transaction_type", T.StringType(), True),
        T.StructField("merchant_category", T.StringType(), True),
        T.StructField("merchant_subcategory", T.StringType(), True),
        T.StructField("amount", T.StringType(), True),
        T.StructField("currency", T.StringType(), True),
        T.StructField("channel", T.StringType(), True),
        T.StructField(
            "location",
            T.StructType(
                [
                    T.StructField("province", T.StringType(), True),
                    T.StructField("city", T.StringType(), True),
                    T.StructField("coordinates", T.StringType(), True),
                ]
            ),
            True,
        ),
        T.StructField(
            "metadata",
            T.StructType(
                [
                    T.StructField("device_id", T.StringType(), True),
                    T.StructField("session_id", T.StringType(), True),
                    T.StructField("retry_flag", T.BooleanType(), True),
                ]
            ),
            True,
        ),
    ]
)


def _existing_path(*candidates):
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return str(candidate)
    return str(candidates[0])


def load_config():
    path = _existing_path(
        os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG),
        PROJECT_ROOT / "config" / "pipeline_config.yaml",
    )
    with open(path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    config["_config_path"] = path
    return config


def load_dq_rules():
    path = _existing_path(
        os.environ.get("DQ_RULES_CONFIG", DEFAULT_DQ_RULES),
        PROJECT_ROOT / "config" / "dq_rules.yaml",
    )
    if not Path(path).exists():
        return {"rules": {}}
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {"rules": {}}


def infer_stage(config=None):
    explicit = os.environ.get("PIPELINE_STAGE")
    if explicit in {"1", "2", "3"}:
        return explicit
    stream_dir = Path("/data/stream")
    if stream_dir.exists() and any(stream_dir.glob("stream_*.jsonl")):
        return "3"
    rules_path = Path(DEFAULT_DQ_RULES)
    if rules_path.exists():
        return "2"
    return "1"


def spark_session(config=None):
    config = config or load_config()
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    pyspark_home = str(Path(pyspark.__file__).resolve().parent)
    if not Path(os.environ.get("SPARK_HOME", "")).exists():
        os.environ["SPARK_HOME"] = pyspark_home
    spark_config = config.get("spark", {})
    builder = (
        SparkSession.builder.appName(spark_config.get("app_name", "nedbank-de-pipeline"))
        .master(spark_config.get("master", "local[2]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.default.parallelism", os.environ.get("SPARK_DEFAULT_PARALLELISM", "16"))
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def output_path(config, layer, table=None):
    root = config["output"][f"{layer}_path"]
    return f"{root}/{table}" if table else root


def write_delta(df, path, mode="overwrite"):
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )


def read_delta(spark, path):
    return spark.read.format("delta").load(path)


def parse_date(col):
    s = F.trim(col.cast("string"))
    epoch_seconds = F.when(s.rlike(r"^\d{13}$"), (s.cast("double") / F.lit(1000))).otherwise(s.cast("double"))
    return F.coalesce(
        F.to_date(s, "yyyy-MM-dd"),
        F.to_date(s, "dd/MM/yyyy"),
        F.to_date(F.from_unixtime(epoch_seconds.cast("long"))),
    )


def parse_timestamp(date_col, time_col):
    date_value = F.date_format(parse_date(date_col), "yyyy-MM-dd")
    time_value = F.coalesce(F.trim(time_col.cast("string")), F.lit("00:00:00"))
    return F.to_timestamp(F.concat_ws(" ", date_value, time_value), "yyyy-MM-dd HH:mm:ss")


def date_variant_flag(col):
    s = F.trim(col.cast("string"))
    return s.isNotNull() & ~s.rlike(r"^\d{4}-\d{2}-\d{2}$")


def normalise_currency(col):
    s = F.upper(F.trim(col.cast("string")))
    return F.when(s.isin("ZAR", "R", "RANDS", "710"), F.lit("ZAR")).otherwise(s)


def currency_variant_flag(col):
    s = F.upper(F.trim(col.cast("string")))
    return s.isNotNull() & (s != F.lit("ZAR")) & s.isin("R", "RANDS", "710")


def stable_sk(order_col_name, output_col_name):
    return F.row_number().over(Window.orderBy(F.col(order_col_name))).cast("bigint").alias(output_col_name)


def raw_transaction_frame(spark, path, ingestion_timestamp=None):
    raw = spark.read.text(path).withColumnRenamed("value", "raw_json")
    parsed = raw.withColumn("event", F.from_json("raw_json", TRANSACTION_SCHEMA))
    df = parsed.select("event.*", "raw_json")
    ingestion_col = ingestion_timestamp if ingestion_timestamp is not None else F.to_timestamp(F.lit(RUN_TIMESTAMP.replace("Z", "")))
    return df.withColumn(
        "amount_was_quoted",
        F.col("raw_json").rlike(r'"amount"\s*:\s*"'),
    ).withColumn(
        "ingestion_timestamp",
        ingestion_col,
    )


def add_missing_columns(df, columns):
    for name, data_type in columns.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(data_type))
    return df


def write_json_report(config, report):
    path = Path(config["output"]["dq_report_path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=False)


def execution_seconds():
    return int(time.time() - RUN_STARTED_AT)
