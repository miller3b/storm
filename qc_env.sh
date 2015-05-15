 #!/bin/bash
 # Usage: source qc_env.sh
export WORKING_DIR=/usr/hdp/2.2.0.0-2041

export MAVEN_HOME=~/apache-maven-3.3.3

export KAFKA_HOME=$WORKING_DIR/storm
export STORM_HOME=$WORKING_DIR/kafka
export HADOOP_HOME=$WORKING_DIR/hadoop
export HIVE_HOME=$WORKING_DIR/hive

export PATH=$PATH:$MAVEN_HOME/bin:$KAFKA_HOME/bin:$STORM_HOME/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin