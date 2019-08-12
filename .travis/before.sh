#!/usr/bin/env bash
export HBASE_DOWNLOAD_FILE="$HOME/Downloads/hbase-1.2.4-bin.tar.gz"
export HBASE_FILES="hbase-1.2.4"

mkdir /home/travis/hbase /home/travis/zookeeper
if [[ ! -d $HOME/Downloads/hbase-1.2.4-bin ]];
then
    echo "Downloading HBase"
    sudo wget -O $HBASE_DOWNLOAD_FILE https://archive.apache.org/dist/hbase/1.2.4/hbase-1.2.4-bin.tar.gz
    echo "Extracting HBase Files"
    tar -xvzf $HBASE_DOWNLOAD_FILE
fi
echo "Copying HBase Files"
cp -r $HBASE_DOWNLOAD_FILE $HBASE_FILES
echo "Config ..."
cp .travis/hbase-site.xml $HBASE_FILES/conf/
echo "Running HBase"
#echo "export HBASE_HOME=/home/travis/build/nimbo3/Keenbo/hbase-1.2.4" >> /home/travis/.bashrc
#source /home/travis/.bashrc
#sudo chmod -R 777 hbase-1.2.4
sudo $HBASE_FILES/bin/start-hbase.sh
echo "Preparing HBase"
echo 'list' | $HBASE_FILES/bin/hbase shell -n
#echo 'create "page", "meta", "anchor"' | hbase-1.2.4/bin/hbase shell -n
#echo 'list' | hbase-1.2.4/bin/hbase shell -n
echo "HBase is ready"
