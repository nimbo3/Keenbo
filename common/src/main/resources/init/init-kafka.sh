#!/bin/bash

check_status () {
  status=$?
  if [ $status -ne 0 ]
  then
    echo "Unable to initialzie kafka"
  else
    echo "Kafka initialized successfully"
  fi
}

echo "Delete kafka topic: $KAFKA_TOPIC_LINKS"
/var/local/kafka/bin/kafka-topics.sh --delete --topic $KAFKA_TOPIC_LINKS --zookeeper $ZOOKEEPER_HOST:2181
echo ""
echo "Delete kafka topic: $KAFKA_TOPIC_PAGES"
/var/local/kafka/bin/kafka-topics.sh --delete --topic $KAFKA_TOPIC_PAGES --zookeeper $ZOOKEEPER_HOST:2181
echo ""
echo "Delete kafka topic: $KAFKA_TOPIC_SHUFFLER"
/var/local/kafka/bin/kafka-topics.sh --delete --topic $KAFKA_TOPIC_SHUFFLER --zookeeper $ZOOKEEPER_HOST:2181
echo ""
sleep 5

echo "Create kafka topic: $KAFKA_TOPIC_LINKS"
/var/local/kafka/bin/kafka-topics.sh --create --topic $KAFKA_TOPIC_LINKS --partitions 21 --replication-factor 2 --zookeeper $ZOOKEEPER_HOST:2181
check_status
echo ""
echo "Create kafka topic: $KAFKA_TOPIC_PAGES"
/var/local/kafka/bin/kafka-topics.sh --create --topic $KAFKA_TOPIC_PAGES --partitions 21 --replication-factor 2 compression.type=gzip --zookeeper $ZOOKEEPER_HOST:2181
check_status
echo ""
echo "Create kafka topic: $KAFKA_TOPIC_SHUFFLER"
/var/local/kafka/bin/kafka-topics.sh --create --topic $KAFKA_TOPIC_SHUFFLER --partitions 21 --replication-factor 2 --zookeeper $ZOOKEEPER_HOST:2181
check_status



