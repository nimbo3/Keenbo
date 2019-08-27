#!/bin/bash

export BASEDIR=$(dirname "$0")
cd $BASEDIR

source ./keenbo-env.sh

echo 'DELETE old elasticsearch index'
curl -XDELETE "http://$ELASTICSEARCH_NODE:9200/$ELASTICSEARCH_INDEX" >/dev/null
sleep 2

echo "--------------------------------------------------------------------------------"
echo 'Create elasticsearch index'
curl -XPUT "http://$ELASTICSEARCH_NODE:9200/$ELASTICSEARCH_INDEX" -H 'Content-Type: application/json' -d'
  "settings": {
    "index": {
      "number_of_shards": 6,
      "number_of_replicas": 1
    },
    "analysis": {
      "analyzer": {
        "custom_analyzer": {
          "type": "custom",
          "char_filter": [
            "html_strip"
          ],
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "english_stop"
          ]
        }
      },
      "filter": {
        "english_stop": {
          "type": "stop",
          "stopwords": [
            "his",
            "did",
            "up",
            "you",
            "each",
            "most",
            "nor",
            "while",
            "now",
            "wants",
            "me",
            "these",
            "its",
            "only",
            "own",
            "dear",
            "would",
            "very",
            "don",
            "under",
            "t",
            "them",
            "with",
            "tis",
            "ever",
            "because",
            "few",
            "your",
            "from",
            "ours",
            "into",
            "may",
            "been",
            "my",
            "out",
            "cannot",
            "us",
            "least",
            "might",
            "rather",
            "below",
            "as",
            "said",
            "of",
            "himself",
            "can",
            "should",
            "and",
            "must",
            "else",
            "do",
            "their",
            "almost",
            "often",
            "to",
            "him",
            "was",
            "had",
            "if",
            "before",
            "against",
            "when",
            "across",
            "got",
            "again",
            "on",
            "then",
            "her",
            "not",
            "where",
            "what",
            "get",
            "am",
            "an",
            "any",
            "after",
            "however",
            "since",
            "doing",
            "our",
            "able",
            "like",
            "there",
            "who",
            "myself",
            "for",
            "above",
            "hers",
            "every",
            "neither",
            "no",
            "or",
            "about",
            "through",
            "yourself",
            "say",
            "itself",
            "were",
            "being",
            "down",
            "in",
            "themselves",
            "says",
            "they",
            "over",
            "it",
            "than",
            "also",
            "yours",
            "so",
            "let",
            "by",
            "the",
            "likely",
            "some",
            "s",
            "off",
            "both",
            "has",
            "once",
            "i",
            "here",
            "more",
            "such",
            "she",
            "all",
            "herself",
            "which",
            "until",
            "we",
            "same",
            "among",
            "but",
            "other",
            "are",
            "at",
            "he",
            "whom",
            "how",
            "a",
            "will",
            "yet",
            "yourselves",
            "be",
            "too",
            "have",
            "theirs",
            "further",
            "this",
            "does",
            "is",
            "during",
            "either",
            "could",
            "that",
            "between",
            "twas",
            "why",
            "those",
            "having",
            "just",
            "ourselves"
          ]
        }
      }
    }
  },
  "mappings": {
    "page": {
      "properties": {
        "content": {
          "type": "text",
          "term_vector" : "yes",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          },
          "analyzer" : "custom_analyzer"
        },
        "link": {
          "type": "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 2048
            }
          },
          "analyzer" : "custom_analyzer"
        },
        "forward_count": {
          "type": "long"
        },
        "link_depth": {
          "type": "long"
        },
        "meta": {
          "properties": {
            "content": {
              "type": "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              },
              "analyzer" : "custom_analyzer"
            },
            "key": {
              "type": "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              },
              "analyzer" : "custom_analyzer"
            }
          }
        },
        "rank": {
          "type": "double"
        },
        "title": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "analyzer" : "custom_analyzer"
      }
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


