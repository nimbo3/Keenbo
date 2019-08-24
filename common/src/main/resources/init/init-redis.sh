#!/bin/bash

echo 'Clear Redis history'
for host in "${hosts[@]}"
do
   ssh -p 3031 root@$host 'redis-cli flushall'
done
echo 'Redis history cleared'

