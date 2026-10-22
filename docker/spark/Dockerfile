# cryptopipe-spark: Spark 3.5.9 (Scala 2.12, Java 17) + Python 3.11 + connector jars + the cryptopipe wheel.
# Owner: A (Platform). Used by every SparkApplication and by the History Server.
# Build from the repo root:  make build-images   (or: docker build -f docker/spark/Dockerfile -t cryptopipe-spark .)
#
# Why not FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu: that image ships Python 3.10 and cryptopipe
# needs 3.11 (requires-python, datetime.UTC, StrEnum); driver and executors must run the package's Python. So the
# Spark distribution and its entrypoint are copied from the official JVM image onto python:3.11.
# Connector jars are pinned in jars.txt and verified at build time; pods never download jars.
ARG SPARK_VERSION=3.5.9
ARG SPARK_IMAGE=apache/spark:${SPARK_VERSION}-scala2.12-java17-ubuntu
ARG PYTHON_IMAGE=python:3.11-slim-bookworm

FROM ${SPARK_IMAGE} AS spark

FROM ${PYTHON_IMAGE} AS jars
COPY docker/spark/jars.txt docker/spark/fetch_jars.py /build/
RUN python /build/fetch_jars.py /build/jars.txt /jars

FROM ${PYTHON_IMAGE} AS wheel
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip wheel --no-cache-dir --no-deps -w /wheels .

FROM ${PYTHON_IMAGE}
LABEL org.opencontainers.image.title="cryptopipe-spark" \
      org.opencontainers.image.description="Spark 3.5.9 + Kafka/Cassandra/GraphFrames jars + cryptopipe"

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless tini procps bash libnss3 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 185 spark && useradd --system --uid 185 --gid spark --home-dir /opt/spark spark

COPY --from=spark /opt/spark /opt/spark
COPY --from=spark /opt/entrypoint.sh /opt/decom.sh /opt/
ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=/opt/spark/bin:$PATH \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11 \
    HADOOP_CONF_DIR=/opt/hadoop-conf \
    PYTHONUNBUFFERED=1

# Use the Spark distribution's own pyspark (no second copy from pip).
RUN python -c "import site, glob; open(site.getsitepackages()[0] + '/spark.pth', 'w').write('/opt/spark/python\n' + glob.glob('/opt/spark/python/lib/py4j-*-src.zip')[0] + '\n')"

COPY --from=jars /jars/ /opt/spark/jars/

# Runtime dependencies come from the wheel; pyspark itself is provided by /opt/spark/python (see spark.pth).
COPY --from=wheel /wheels /tmp/wheels
RUN pip install --no-cache-dir /tmp/wheels/*.whl && pip uninstall -y pyspark && rm -rf /tmp/wheels /root/.cache

# Image-level Spark defaults (event log, Prometheus, UTC). Job-specific settings live in the SparkApplication YAMLs.
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY conf/symbols.yaml /opt/cryptopipe/conf/symbols.yaml
COPY docker/spark/run_job.py docker/spark/verify_jars.py /opt/cryptopipe/
COPY docker/spark/gen_entrypoints.py /tmp/gen_entrypoints.py
# HADOOP_CONF_DIR is filled at runtime from ConfigMap hadoop-conf (spec.hadoopConfigMap in the SparkApplication)
RUN python /tmp/gen_entrypoints.py /opt/cryptopipe/jobs && rm /tmp/gen_entrypoints.py \
 && mkdir -p /opt/hadoop-conf
ENV CRYPTO_SYMBOLS_FILE=/opt/cryptopipe/conf/symbols.yaml

WORKDIR /opt/spark/work-dir
RUN chown spark:spark /opt/spark/work-dir && chmod g+w /opt/spark/work-dir
USER 185
ENTRYPOINT ["/opt/entrypoint.sh"]
