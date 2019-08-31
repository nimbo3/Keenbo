#!/bin/bash

export BASEDIR=$(dirname "$0")
cd BASEDIR

source ./keenbo-env.sh

# initialize HBase table
bash ./init-hbase.sh
echo "--------------------------------------------------------------------------------"

# initialize ElasticSearch index
bash ./init-elasticsearch.sh
echo "--------------------------------------------------------------------------------"

# initialize kafka
bash ./init-kafka.sh
echo "--------------------------------------------------------------------------------"

# initialize redis
bash ./init-redis.sh
echo "--------------------------------------------------------------------------------"

