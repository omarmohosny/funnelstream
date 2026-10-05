FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-17 \
    && java -version
ENV JAVA_HOME=/usr/lib/jvm/java-17

RUN pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.2.0 pyyaml==6.0.1 \
    pytest==8.2.2 jsonschema==4.22.0 confluent-kafka==2.4.0 \
    databricks-sdk==0.29.0 databricks-sql-connector==3.3.0 pyarrow==16.1.0

WORKDIR /app
ENV PYTHONPATH=/app

# Runs as root, so files written under data/ are owned by root on the host mount.
