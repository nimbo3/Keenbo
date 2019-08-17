#!/usr/bin/env bash
BASEDIR=$(dirname "$0")
cd "$BASEDIR"
cd ../conf
jar -uf ../lib/search-1.0.jar *
if [ -d "../logs" ];
then
    cd ../logs
else
    mkdir ../logs
    cd ../logs
fi
java -jar ../lib/search-1.0.jar
