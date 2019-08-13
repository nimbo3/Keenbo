#!/usr/bin/env bash
BASEDIR=$(dirname "$0")
cd "$BASEDIR"
cd ../conf
jar -uf ../lib/crawler-1.0.jar *
if [[ -d "../logs" ]];
then
    cd ../logs
else
    mkdir ../logs
    cd ../logs
fi

export JAVA_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9070 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=localhost"
java $JAVA_OPTS -javaagent:../lib/jmx_prometheus.jar=9101:../conf/jmx-promethues.yaml -jar ../lib/crawler-1.0.jar