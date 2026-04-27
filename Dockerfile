FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies you need beyond the base image.
# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app
ENV PYTHONPATH=/app
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV SPARK_LOCAL_IP=127.0.0.1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; spark = configure_spark_with_delta_pip(SparkSession.builder.master('local[1]').appName('cache-delta-jars').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')).getOrCreate(); spark.stop()"
RUN mkdir -p /opt/delta-jars && cp /root/.ivy2/jars/*.jar /opt/delta-jars/

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
COPY pipeline/ pipeline/
COPY config/ config/

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "pipeline/run_all.py"]
