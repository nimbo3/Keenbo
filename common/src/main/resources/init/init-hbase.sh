#!/bin/bash

export BASEDIR=$(dirname "$0")
cd $BASEDIR

source ./keenbo-env.sh

echo 'Create Hbase Table'
echo "create '$HBASE_TABLE', 'D', 'A'" | hbase shell -n >/dev/null

echo 'Truncating Hbase Table'
echo "truncate '$HBASE_TABLE'" | hbase shell -n >/dev/null
status=$?
if [ $status -ne 0 ]
then
	echo "Unable to truncate table $HBASE_TABLE"
else
	echo 'HBase table Truncated'
fi