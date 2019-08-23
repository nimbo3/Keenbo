#!/bin/bash

export ZOOKEEPER_HOST="slave-1"
export HBASE_TABLE="page"
export ELASTICSEARCH_NODE="localhost"
export ELASTICSEARCH_INDEX="search"
export KAFKA_TOPIC_LINKS="links"
export KAFKA_TOPIC_SHUFFLER="shuffler"
export KAFKA_TOPIC_PAGES="pages"

declare -a hosts=("slave-1" "slave-2" "slave-3")