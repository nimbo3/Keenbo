#!/usr/bin/env bash
export HBASE_DOWNLOAD_FILE="$HOME/Downloads/hbase-1.2.4-bin.tar.gz"
export JDK_DOWNLOAD_FILE="$HOME/Downloads/jdk.tar.gz"
export HBASE_FILES="hbase-1.2.4"

mkdir /home/travis/hbase /home/travis/zookeeper
if [[ ! -d $HOME/Downloads/hbase-1.2.4-bin ]];
then
    echo "Downloading HBase"
    sudo wget -O $HBASE_DOWNLOAD_FILE https://archive.apache.org/dist/hbase/1.2.4/hbase-1.2.4-bin.tar.gz
    echo "Downloading JDK"
    sudo wget -O JDK_DOWNLOAD_FILE http://dl.yasdl.com/Arash/2018/Software/jdk-8u181-linux-x64.tar.gz
fi
echo "Copying HBase Files"
cp $HBASE_DOWNLOAD_FILE hbase-1.2.4.tar.gz
echo "Copying JDK Files"
cp JDK_DOWNLOAD_FILE jdk.tar.gz
echo "Extracting HBase Files"
tar -xvzf hbase-1.2.4.tar.gz
echo "Extracting JDK Files"
tar -xvzf jdk.tar.gz

# HBase configuration
echo "HBase Configuration"
#export JAVA_HOME=$HOME/build/nimbo3/Keenbo/jdk
#export HBASE_HOME="$HOME/build/nimbo3/Keenbo/$HBASE_FILES"
echo $JAVA_HOME
ll $JAVA_HOME
cp .travis/hbase-site.xml $HBASE_FILES/conf/
echo "export JAVA_HOME=/home/travis/build/nimbo3/Keenbo/jdk" >> $HBASE_FILES/conf/hbase-env.sh

echo "Running HBase"
#source /home/travis/.bashrc
#sudo chmod -R 777 hbase-1.2.4
sudo $HBASE_FILES/bin/start-hbase.sh
echo "Preparing HBase"
echo 'list' | $HBASE_FILES/bin/hbase shell -n
#echo 'create "page", "meta", "anchor"' | hbase-1.2.4/bin/hbase shell -n
#echo 'list' | hbase-1.2.4/bin/hbase shell -n
echo "HBase is ready"
