FROM flink:1.20.5-scala_2.12-java11

ARG FLINK_VERSION=1.20.5
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG JDBC_CONNECTOR_VERSION=3.3.0-1.20
ARG POSTGRES_DRIVER_VERSION=42.7.4

SHELL ["/bin/bash", "-ceuxo", "pipefail"]

# PyFlink and pemja ship no Linux arm64 wheels, so on Apple Silicon they are
# compiled from source, which needs Python headers and a full JDK (jni.h).
RUN apt-get update -y && \
  apt-get install -y --no-install-recommends \
    python3 python3-dev python3-pip build-essential openjdk-11-jdk-headless && \
  ln -sf /usr/bin/python3 /usr/bin/python && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN JAVA_HOME="/usr/lib/jvm/java-11-openjdk-$(dpkg --print-architecture)" \
  pip3 install --no-cache-dir "apache-flink==${FLINK_VERSION}" psycopg2-binary

RUN wget -q -P /opt/flink/lib/ \
  "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
  "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar" \
  "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar"

COPY jdbc_compat.py flink_job_ema.py flink_job_price.py flink_job_same_window.py flink_job_topic_read.py /opt/

CMD ["jobmanager"]
