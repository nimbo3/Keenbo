#!/usr/bin/env bash
ssh -p 3031 root@master '
export JAVA_HOME=/var/local/jdk;
cd /root/project/ehsan;
rm -r Keenbo 2>/dev/null;
echo "cloning ...";
git clone https://github.com/nimbo3/Keenbo > /dev/null;
echo "cloned";
cd Keenbo;
echo "packaging ...";
mvn package -DskipTests > /dev/null;
cp /root/graph/* /root/ehsan/project/Keenbo/target/conf/;
echo "packaged";
rm -r /root/Keenbo;
cp -r target /root/Keenbo;
cd /root;
source bin/init/keenbo-env.sh
echo "uploading";
chmod 777 bin/*;
for host in "${hosts[@]}"
do
    ssh -p 3031 root@$host "cd /root; rm -r Keenbo 2>/dev/null";
	scp -r -P 3031 Keenbo root@$host:/root/Keenbo;
	echo "uploaded to $host";
done
'
