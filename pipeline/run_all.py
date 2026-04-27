from pipeline.common import cleanup_spark_local_dir, infer_stage, load_config, spark_session
from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.stream_ingest import run_stream_ingestion
from pipeline.transform import run_transformation


def main():
    config = load_config()
    spark = spark_session(config)
    try:
        run_ingestion()
        run_transformation()
        run_provisioning()
        if infer_stage(config) == "3":
            run_stream_ingestion()
    finally:
        spark.stop()
        cleanup_spark_local_dir(config)


if __name__ == "__main__":
    main()
