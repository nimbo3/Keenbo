#!/usr/bin/env bash
if [[ ! -f $HOME/downloads/hbase-1.2.4-bin.tar.gz ]];
then
    sudo wget -O $HOME/downloads/hbase-1.2.4-bin.tar.gz https://archive.apache.org/dist/hbase/1.2.4/hbase-1.2.4-bin.tar.gz
fi
sudo mv $HOME/downloads/hbase-1.2.4-bin.tar.gz hbase-1.2.4-bin.tar.gz
sudo tar -xvzf hbase-1.2.4-bin.tar.gz
sudo mv .travis/hbase-site.xml hbase-1.2.4/conf
sudo hbase-1.2.4/bin/start-hbase.sh
