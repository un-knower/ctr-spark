#!/bin/bash

beginLine=`grep -n "trees 0" run_v.log|awk -F':' '{printf("%d",$1)}'`
endLine=`grep -n "SparkContext: Invoking stop() " run_v.log|awk -F':' '{printf("%d",$1)}'`
head -n $((endLine-1)) run_v.log|tail -n $((endLine-beginLine)) > tree.log

java -cp target/ctr-spark.jar com.mtty.tool.ShowTree

nlines=$(wc -l tree.structure |awk -F' ' '{printf("%d",$1)}') && cat tree.structure |awk -F '\\:\\[' -v nlines=$nlines 'BEGIN{printf("var tree={");}{if(NR!=nlines){printf("\"%s\":[%s,",$1,$2)}else{printf("\"%s\":[%s",$1,$2);}}END{printf("};")}' >tree.json

