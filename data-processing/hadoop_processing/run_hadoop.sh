#!/bin/bash

# Hadoop configuration
export HADOOP_HOME=/path/to/hadoop
export HADOOP_CONF_DIR=/path/to/hadoop/conf

# Set input and output paths
INPUT_DIR=/path/to/input/dir
OUTPUT_DIR=/path/to/output/dir

# Run MapReduce job
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
    -D mapreduce.job.reduces=1 \
    -input $INPUT_DIR \
    -output $OUTPUT_DIR \
    -mapper "python /path/to/data_processing/hadoop_processing/logs_mapper.py" \
    -reducer "python /path/to/data_processing/hadoop_processing/logs_reducer.py"
