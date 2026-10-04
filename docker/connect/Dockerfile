FROM eclipse-temurin:21-jre-jammy
ARG KAFKA_VERSION=3.8.0
ARG DEBEZIUM_VERSION=3.0.0.Final
ARG AIVEN_VERSION=3.4.3
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/kafka /opt/plugins/debezium /opt/plugins/aiven \
    && curl -fsSL --retry 5 "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz" -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz --strip-components=1 -C /opt/kafka \
    && curl -fsSL --retry 5 "https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" -o /tmp/debezium.tgz \
    && tar -xzf /tmp/debezium.tgz --strip-components=1 -C /opt/plugins/debezium \
    && curl -fsSL --retry 5 "https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka/releases/download/v${AIVEN_VERSION}/s3-sink-connector-for-apache-kafka-${AIVEN_VERSION}.tar" -o /tmp/aiven.tar \
    && tar -xf /tmp/aiven.tar --strip-components=1 -C /opt/plugins/aiven \
    && rm /tmp/kafka.tgz /tmp/debezium.tgz /tmp/aiven.tar \
    && useradd --system --uid 10001 --create-home connect \
    && chown -R connect:connect /opt/kafka /opt/plugins
COPY worker.properties /opt/kafka/config/cdc-worker.properties
USER connect
EXPOSE 8083
ENTRYPOINT ["/opt/kafka/bin/connect-distributed.sh", "/opt/kafka/config/cdc-worker.properties"]
