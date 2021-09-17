FROM debian:jessie

COPY build/kafka_httpcat /usr/bin/
COPY docker-entrypoint.sh /
RUN chmod +x /usr/bin/kafka_httpcat /docker-entrypoint.sh
ENTRYPOINT [ "/docker-entrypoint.sh" ]
CMD [ "kafka_httpcat" , "kafka-2.local.jumpy:9092", "tug_metrics_kafka2", "metrics"]