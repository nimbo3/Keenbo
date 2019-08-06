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
export here=$(pwd)
export HBASE_HOME=$here/hbase-1.2.4
export PATH=$PATH:$HBASE_HOME/bin
sudo start-hbase.sh
echo "Preparing HBase"
echo 'create "page", "meta", "anchor"' | sudo hbase shell -n
echo "HBase is ready"
