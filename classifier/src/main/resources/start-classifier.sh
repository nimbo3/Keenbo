#!/usr/bin/env bash
export BASEDIR=$(dirname "$0")
export BASEDIR=$BASEDIR/..
export APP_NAME="classifier-1.0.jar"
export LOG_NAME="classifier-logs.txt"
export JMX_PORT="9071"
export PROMETHEUS_PORT="9107"
export JAVA_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=$JMX_PORT -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=localhost"

cd $BASEDIR

jar uf lib/$APP_NAME -C conf/ .
mkdir -p logs
cd logs
rm $LOG_NAME > /dev/null 2>&1

java $JAVA_OPTS -javaagent:../lib/jmx_prometheus.jar=$PROMETHEUS_PORT:../conf/jmx-promethues.yaml -jar ../lib/$APP_NAME