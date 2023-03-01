#!/bin/bash

# Remove output directory if exists
hdfs dfs -rm -r hadoop_output

# Run Hadoop streaming job
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
-files logs_mapper.py,logs_reducer.py \
-mapper logs_mapper.py \
-reducer logs_reducer.py \
-input hadoop_input/NASA_access_log_Jul95.gz \
-output hadoop_output
