#!/bin/bash

export BASEDIR=$(dirname "$0")
cd $BASEDIR

source ./keenbo-env.sh

echo 'Clear Redis history'
for host in "${hosts[@]}"
do
   ssh -p 3031 root@$host 'redis-cli flushall'
done
echo 'Redis history cleared'

