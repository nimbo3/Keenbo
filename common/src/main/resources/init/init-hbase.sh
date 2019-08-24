#!/bin/bash

echo 'Truncating Hbase Table'
echo "truncate \"$HBASE_TABLE\"" | hbase shell -n >/dev/null
status=$?
if [ $status -ne 0 ]
then
	echo "Unable to truncate table page"
else
	echo 'HBase table Truncated'
fi