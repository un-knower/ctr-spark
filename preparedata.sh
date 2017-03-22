#!/bin/bash

nohup /usr/hdp/current/spark2-client/bin/spark-submit --class com.mtty.PrepareData --master yarn --driver-memory 4G --executor-memory 11G --executor-cores 6 --num-executors 11 target/ctr-spark.jar >run_v.log 2>&2 &
