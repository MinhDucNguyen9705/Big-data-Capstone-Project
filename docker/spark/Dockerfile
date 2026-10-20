# Spark image for SparkApplications (first draft from the bootstrap; owner A finalizes).
# Spark 3.5 + Kafka and Cassandra connectors + the cryptopipe wheel.
# Build from the repo root:  docker build -f docker/spark/Dockerfile -t cryptopipe-spark:dev .
#
# Why not FROM apache/spark:3.5.x-python3 directly: that image ships Python 3.10 and cryptopipe needs 3.11
# (requires-python, datetime.UTC, StrEnum). So the Spark distribution and its entrypoint are copied from the
# official JVM image onto python:3.11. Connector jars are resolved at build time; pods never download jars.
ARG SPARK_VERSION=3.5.9
ARG SPARK_IMAGE=apache/spark:${SPARK_VERSION}-scala2.12-java17-ubuntu

FROM ${SPARK_IMAGE} AS spark

FROM python:3.11-slim-bookworm AS wheel
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip wheel --no-deps -w /wheels .

FROM python:3.11-slim-bookworm
ARG SPARK_VERSION=3.5.9
ARG CASSANDRA_CONNECTOR_VERSION=3.5.1

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless tini procps bash libnss3 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 185 spark && useradd --system --uid 185 --gid spark --home-dir /opt/spark spark

COPY --from=spark /opt/spark /opt/spark
COPY --from=spark /opt/entrypoint.sh /opt/decom.sh /opt/
ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=/opt/spark/bin:$PATH \
    PYSPARK_PYTHON=python3

# Use the Spark distribution's own pyspark (no second copy from pip).
RUN python -c "import site, glob; open(site.getsitepackages()[0] + '/spark.pth', 'w').write('/opt/spark/python\n' + glob.glob('/opt/spark/python/lib/py4j-*-src.zip')[0] + '\n')"

# Resolve the connectors (with transitive deps) with Spark's ivy resolver at build time.
# Scala and slf4j jars from ivy are dropped: Spark already ships them.
RUN echo 'print("resolved")' > /tmp/noop.py \
 && spark-submit --master 'local[1]' --conf spark.jars.ivy=/tmp/ivy \
      --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION},com.datastax.spark:spark-cassandra-connector-assembly_2.12:${CASSANDRA_CONNECTOR_VERSION}" \
      /tmp/noop.py \
 && rm -f /tmp/ivy/jars/org.scala-lang_* /tmp/ivy/jars/org.slf4j_* \
 && cp -n /tmp/ivy/jars/*.jar /opt/spark/jars/ \
 && rm -rf /tmp/ivy /tmp/noop.py /root/.ivy2* /root/.cache

# Dependencies come from the wheel; pyspark itself is provided by /opt/spark/python (see spark.pth).
COPY --from=wheel /wheels /tmp/wheels
RUN pip install --no-cache-dir /tmp/wheels/*.whl && pip uninstall -y pyspark && rm -rf /tmp/wheels

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY conf/symbols.yaml /opt/cryptopipe/conf/symbols.yaml
COPY docker/spark/run_job.py /opt/cryptopipe/run_job.py
ENV CRYPTO_SYMBOLS_FILE=/opt/cryptopipe/conf/symbols.yaml

WORKDIR /opt/spark/work-dir
RUN chown spark:spark /opt/spark/work-dir && chmod g+w /opt/spark/work-dir
USER 185
ENTRYPOINT ["/opt/entrypoint.sh"]
