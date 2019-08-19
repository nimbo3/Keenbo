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
echo "packaged";
i=1
while [ $i -lt 4 ];
do
	ssh -p 3031 root@slave-$i "cd project/ehsan/Keenbo; rm -r target 2>/dev/null"
	let i=i+1
done
echo "uploading";
let i=1
while [ $i -lt 4 ];
do
	scp -r -P 3031 target root@slave-$i:/root/project/ehsan/Keenbo/
	echo "uploaded to slave-$i"
        let i=i+1
done
'
