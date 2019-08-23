#!/bin/bash

echo 'DELETE old elasticsearch index'
curl -XDELETE "http://$ELASTICSEARCH_NODE:9200/$ELASTICSEARCH_INDEX" >/dev/null
sleep 2

echo "--------------------------------------------------------------------------------"
echo 'Create elasticsearch index'
curl -XPUT "http://$ELASTICSEARCH_NODE:9200/$ELASTICSEARCH_INDEX" -H 'Content-Type: application/json' -d'
{
    "settings" : {
        "index" : {
            "number_of_shards" : 6,
            "number_of_replicas" : 1
        }
    }
}' >/dev/null

status=$?
if [ $status -ne 0 ]
then
	echo "Unable to initialzie Elasticsearch"
else
	echo 'ElasticSearch initialized'
fi