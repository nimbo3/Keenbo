#!/usr/bin/env bash
source ../init/keenbo-env.sh
wget https://www-eu.apache.org/dist/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz
tar -xvzf hadoop-2.7.7.tar.gz
rm -r /var/local/hadoop
mv hadoop-2.7.7 /var/local/hadoop
echo '
export HADOOP_HOME=/var/local/hadoop
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HDFS_NAMENODE_USER="root"
export HDFS_DATANODE_USER="root"
export HDFS_SECONDARYNAMENODE_USER="root"
export YARN_RESOURCEMANAGER_USER="root"
export YARN_NODEMANAGER_USER="root"
' >> ~/.bashrc
source ~/.bashrc
cd /var/local/hadoop
cd etc/hadoop/
echo '
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
   <name>fs.default.name</name>
   <value>hdfs://master:9000</value>
</property>
</configuration>
' > core-site.xml
echo '
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
    <name>dfs.namenode.name.dir</name>
    <value>/var/local/hadoop/data/nameNode</value>
</property>
<property>
     <name>dfs.datanode.data.dir</name>
     <value>/var/local/hadoop/data/dataNode</value>
</property>
<property>
      <name>dfs.replication</name>
      <value>2</value>
</property>
</configuration>
' > hdfs-site.xml
echo '
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
     <name>mapreduce.framework.name</name>
     <value>yarn</value>
</property>
</configuration>
' > mapred-site.xml
echo '
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
    <name>yarn.acl.enable</name>
    <value>0</value>
</property>
<property>
    <name>yarn.resourcemanager.hostname</name>
    <value>master</value>
</property>
<property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
</property>
</configuration>
' > yarn-site.xml
echo "export JAVA_HOME=/var/local/jdk" >> hadoop-env.sh
echo 'export HADOOP_SSH_OPTS="-p 3031"' >> hadoop-env.sh
echo "" > slaves
for host in "${hosts[@]}"
do
	echo $host >> slaves
done
for host in "${hosts[@]}"
do
	scp -P 3031 -r /var/local/hadoop root@$host:/var/local/hadoop
done
hdfs namenode -format
start-all.sh
