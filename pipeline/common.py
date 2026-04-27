import json
import os
import shutil
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
DELTA_WRITE_METRICS = []

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
    config_path = None
    try:
        config_path = (load_config().get("dq") or {}).get("rules_path")
    except FileNotFoundError:
        config_path = None
    path = _existing_path(
        os.environ.get("DQ_RULES_CONFIG", config_path or DEFAULT_DQ_RULES),
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


def _spark_local_dir(config):
    spark_config = (config or {}).get("spark", {})
    configured = spark_config.get("local_dir") or os.environ.get("SPARK_LOCAL_DIR")
    if configured:
        return configured, False
    if Path("/data/output").exists() and os.access("/data/output", os.W_OK):
        return "/data/output/_spark_tmp", True
    return "/tmp", False


def cleanup_spark_local_dir(config=None):
    path, managed = _spark_local_dir(config or {})
    if not managed:
        return
    output_root = Path((config or {}).get("output", {}).get("gold_path", "/data/output/gold")).parent.resolve()
    target = Path(path).resolve()
    if target.name == "_spark_tmp" and (target == output_root / "_spark_tmp" or output_root in target.parents):
        shutil.rmtree(target, ignore_errors=True)


def spark_session(config=None):
    config = config or load_config()
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    pyspark_home = str(Path(pyspark.__file__).resolve().parent)
    if not Path(os.environ.get("SPARK_HOME", "")).exists():
        os.environ["SPARK_HOME"] = pyspark_home
    spark_config = config.get("spark", {})
    parquet_compression = os.environ.get("SPARK_PARQUET_COMPRESSION") or spark_config.get("parquet_compression", "snappy")
    spark_local_dir, _ = _spark_local_dir(config)
    builder = (
        SparkSession.builder.appName(spark_config.get("app_name", "nedbank-de-pipeline"))
        .master(spark_config.get("master", "local[2]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.dir", spark_local_dir)
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.default.parallelism", os.environ.get("SPARK_DEFAULT_PARALLELISM", "4"))
        .config("spark.sql.files.maxPartitionBytes", os.environ.get("SPARK_MAX_PARTITION_BYTES", "67108864"))
        .config("spark.sql.autoBroadcastJoinThreshold", os.environ.get("SPARK_AUTO_BROADCAST_THRESHOLD", "67108864"))
        .config("spark.sql.parquet.compression.codec", parquet_compression)
        .config("spark.sql.adaptive.enabled", os.environ.get("SPARK_SQL_ADAPTIVE", "true"))
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    delta_jars = sorted(Path(os.environ.get("DELTA_JARS_DIR", "/opt/delta-jars")).glob("*.jar"))
    if delta_jars:
        return builder.config("spark.jars", ",".join(str(path) for path in delta_jars)).getOrCreate()
    return configure_spark_with_delta_pip(builder).getOrCreate()


def output_path(config, layer, table=None):
    root = config["output"][f"{layer}_path"]
    return f"{root}/{table}" if table else root


def _latest_delta_operation_metrics(path):
    log_dir = Path(path) / "_delta_log"
    if not log_dir.exists():
        return {}
    for log_file in sorted(log_dir.glob("*.json"), reverse=True):
        with open(log_file, "r", encoding="utf-8") as handle:
            for line in handle:
                action = json.loads(line)
                metrics = action.get("commitInfo", {}).get("operationMetrics")
                if metrics:
                    return metrics
    return {}


def metric_int(metrics, name, default=0):
    value = (metrics or {}).get(name, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _configured_output_partitions(path):
    env_value = os.environ.get("DELTA_OUTPUT_PARTITIONS")
    if env_value is not None:
        return int(env_value)

    try:
        config = load_config()
    except FileNotFoundError:
        config = {}

    partition_config = ((config.get("delta") or {}).get("output_partitions") or {})
    default = int(partition_config.get("default", 4))
    normalized = str(path).replace("\\", "/").rstrip("/")
    parts = [part for part in normalized.split("/") if part]

    for layer in ("bronze", "silver", "gold", "stream_gold"):
        if layer not in parts:
            continue
        layer_index = parts.index(layer)
        table = parts[layer_index + 1] if layer_index + 1 < len(parts) else None
        layer_config = partition_config.get(layer)
        if isinstance(layer_config, dict) and table in layer_config:
            return int(layer_config[table])
        if isinstance(layer_config, int):
            return int(layer_config)

    return default


def write_delta(df, path, mode="overwrite", partition_by=None):
    started_at = time.time()
    status = "ok"
    error = None
    metrics = {}
    output_partitions = _configured_output_partitions(path)
    if output_partitions > 0:
        df = df.coalesce(output_partitions)
    try:
        writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        metrics = _latest_delta_operation_metrics(path)
        return metrics
    except Exception as exc:
        status = "failed"
        error = str(exc)
        raise
    finally:
        entry = {
            "path": path,
            "mode": mode,
            "status": status,
            "duration_seconds": round(time.time() - started_at, 3),
            "output_partitions": output_partitions,
        }
        if partition_by:
            entry["partition_by"] = list(partition_by)
        if metrics:
            entry["operation_metrics"] = metrics
        if error:
            entry["error"] = error
        DELTA_WRITE_METRICS.append(entry)


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
