FROM confluentinc/cp-kafka-connect:7.5.0

USER root

RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4 && \
    confluent-hub install --no-prompt datastax/kafka-connect-cassandra-sink:latest

USER appuser