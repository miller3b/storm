 #!/bin/bash
 # Usage: source local_dev_env.sh
export WORKING_DIR=/home/dnelson/prometric/storm_ingestion

export KAFKA_HOME=$WORKING_DIR/apache-storm-0.9.3
export STORM_HOME=$WORKING_DIR/apache-storm-0.9.3
export HADOOP_HOME=$WORKING_DIR/hadoop-2.6.0
export HIVE_HOME=$WORKING_DIR/apache-hive-0.14.0-bin

export PATH=$PATH:$KAFKA_HOME/bin:$STORM_HOME/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin