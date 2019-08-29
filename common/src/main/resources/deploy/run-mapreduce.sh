#!/usr/bin/env bash
ssh -p 3031 root@master "/var/local/spark/bin/spark-submit --class in.nimbo.App --master spark://master:7077 --deploy-mode cluster /root/Keenbo/lib/$1-1.0-jar-with-dependencies.jar --jars /root/Keenbo/lib/$1-1.0-jar-with-dependencies.jar"
