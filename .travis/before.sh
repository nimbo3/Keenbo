#!/usr/bin/env bash
if [[ ! -f $HOME/downloads/hbase-1.2.4-bin.tar.gz ]];
then
    echo "Downloading"
    sudo wget -O $HOME/downloads/hbase-1.2.4-bin.tar.gz https://archive.apache.org/dist/hbase/1.2.4/hbase-1.2.4-bin.tar.gz
fi
echo "moving"
sudo mv $HOME/downloads/hbase-1.2.4-bin.tar.gz hbase-1.2.4.tar.gz
echo "Extracting"
sudo tar -xvzf hbase-1.2.4.tar.gz
echo "Config ..."
sudo mv .travis/hbase-site.xml hbase-1.2.4/conf
echo "Running HBase"
sudo hbase-1.2.4/bin/start-hbase.sh
echo "Preparing HBase"
echo 'create "links", "meta", "anchor"' | hbase-1.2.4/bin/hbase shell -n >/dev/null 2>&1
echo "HBase is ready"
