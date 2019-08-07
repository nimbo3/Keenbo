#!/usr/bin/env bash
mkdir /home/travis/hbase /home/travis/zookeeper
if [[ ! -f $HOME/Downloads/hbase-1.2.4-bin.tar.gz ]];
then
    echo "Downloading"
    sudo wget -O $HOME/Downloads/hbase-1.2.4-bin.tar.gz https://archive.apache.org/dist/hbase/1.2.4/hbase-1.2.4-bin.tar.gz
fi
echo "moving"
sudo mv $HOME/Downloads/hbase-1.2.4-bin.tar.gz hbase-1.2.4.tar.gz
echo "Extracting"
sudo tar -xvzf hbase-1.2.4.tar.gz
echo "Config ..."
sudo mv .travis/hbase-site.xml hbase-1.2.4/conf
echo "Running HBase"
pwd
export HBASE_HOME=$(pwd)/hbase-1.2.4
echo $HBASE_HOME
hbase-1.2.4/bin/start-hbase.sh
echo "Preparing HBase"
echo 'list' | hbase-1.2.4/bin/hbase shell -n
echo 'create "page", "meta", "anchor"' | hbase-1.2.4/bin/hbase shell -n
echo 'list' | hbase-1.2.4/bin/hbase shell -n
echo "HBase is ready"
