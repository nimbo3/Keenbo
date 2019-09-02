#!/usr/bin/env bash
source ../init/keenbo-env.sh
hadoop fs -mkdir /hbase
wget https://archive.apache.org/dist/hbase/1.2.4/hbase-1.2.4-bin.tar.gz
tar -xvzf hbase-1.2.4-bin.tar.gz
mv hbase-1.2.4-bin /var/local/hbase
echo '
    export HBASE_HOME=/var/local/hbase
    export PATH=$PATH:$HBASE_HOME/bin
' >> ~/.bashrc
source ~/.bashrc
cd /var/local/hbase/conf
echo '
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
    <name>hbase.rootdir</name>
    <value>hdfs://master:9000/hbase</value>
</property>

<property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
</property>

<property>
    <name>hbase.zookeeper.quorum</name>
    <value>master</value>
</property>
</configuration>
' > hbase-site.xml
echo '
    export JAVA_HOME=/var/local/jdk
    export HBASE_SSH_OPTS="-p 3031"
'
echo "" > regionservers
for host in "${hosts[@]}"
do
    echo $host >> regionservers
done
for host in "${hosts[@]}"
do
	scp -P 3031 -r /var/local/hbase root@$host:/var/local/hbase
done
start-hbase.sh
