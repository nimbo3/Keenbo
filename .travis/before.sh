#!/usr/bin/env bash
mkdir /home/travis/hbase /home/travis/zookeeper
if [[ ! -d $HOME/Downloads/hbase-1.2.4-bin ]];
then
    echo "Downloading HBase"
    sudo wget -O $HOME/Downloads/hbase-1.2.4-bin.tar.gz https://archive.apache.org/dist/hbase/1.2.4/hbase-1.2.4-bin.tar.gz
    echo "Extracting HBase Files"
    tar -xvzf hbase-1.2.4-bin.tar.gz
fi
echo "Copying HBase Files"
cp -r $HOME/Downloads/hbase-1.2.4-bin hbase-1.2.4
echo "Config ..."
cp .travis/hbase-site.xml hbase-1.2.4/conf/
echo "Running HBase"
#echo "export HBASE_HOME=/home/travis/build/nimbo3/Keenbo/hbase-1.2.4" >> /home/travis/.bashrc
#source /home/travis/.bashrc
#sudo chmod -R 777 hbase-1.2.4
sudo hbase-1.2.4/bin/start-hbase.sh
echo "Preparing HBase"
#echo 'list' | hbase-1.2.4/bin/hbase shell -n
#echo 'create "page", "meta", "anchor"' | hbase-1.2.4/bin/hbase shell -n
#echo 'list' | hbase-1.2.4/bin/hbase shell -n
echo "HBase is ready"