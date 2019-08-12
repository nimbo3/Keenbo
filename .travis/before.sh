#!/usr/bin/env bash
export HBASE_DOWNLOAD_FILE="$HOME/Downloads/hbase-1.2.4-bin.tar.gz"
export HBASE_FILES="hbase-1.2.4"

mkdir /home/travis/hbase /home/travis/zookeeper
echo "Downloading HBase"
sudo wget -O $HBASE_DOWNLOAD_FILE https://archive.apache.org/dist/hbase/1.2.4/hbase-1.2.4-bin.tar.gz

echo "Copying HBase Files"
cp $HBASE_DOWNLOAD_FILE hbase-1.2.4.tar.gz

echo "Extracting HBase Files"
tar -xvzf hbase-1.2.4.tar.gz > /dev/null 2>&1

echo "HBase Configuration"
cp .travis/hbase-site.xml $HBASE_FILES/conf/
echo "export JAVA_HOME=$JAVA_HOME" >> $HBASE_FILES/conf/hbase-env.sh

echo "Running HBase"
sudo $HBASE_FILES/bin/start-hbase.sh
