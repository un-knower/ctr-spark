#!/bin/bash

hadoop fs -rm tr-svm

nohup /usr/hdp/current/spark2-client/bin/spark-submit --class com.mtty.PrepareData --master yarn --driver-memory 4G --executor-memory 11G --executor-cores 6 --num-executors 11 --jars /usr/hdp/2.5.3.0-37/spark2/jars/spark-mllib_2.11-2.0.0.2.5.3.0-37.jar,/usr/hdp/2.5.3.0-37/spark2/jars/spark-mllib-local_2.11-2.0.0.2.5.3.0-37.jar target/ctr-spark.jar >run_v.log 2>&2 &
