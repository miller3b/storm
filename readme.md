Startup Kafka:
 * cd $KAFKA_HOME; bin/zookeeper-server-start.sh config/zookeeper.properties
 * cd $KAFKA_HOME; bin/kafka-server-start.sh config/server.properties 


Startup Storm:
 * $STORM_HOME/bin/storm nimbus
 * $STORM_HOME/bin/storm ui
 * $STORM_HOME/bin/storm supervisor


 Create the topic:
 ./bin/kafka-topics.sh --create --topic test --partitions 1 --replication-factor 1 --zookeeper localhost:2181

./bin/kafka-console-producer.sh  --broker localhost:9092 --topic test


One-line start kafka & zookeeper & storm & hdfs:
gnome-terminal --working-directory=/home/dnelson/prometric/storm_ingestion/kafka_2.10-0.8.1 \
--tab -t 'zookeeper' -e 'bin/zookeeper-server-start.sh config/zookeeper.properties' \
--tab -t 'kafka' -e 'bin/kafka-server-start.sh config/server.properties' \
--tab -t 'storm nimbus' --working-directory=/home/dnelson/prometric/storm_ingestion/apache-storm-0.9.3  -e 'bin/storm nimbus' \
--tab -t 'storm ui' --working-directory=/home/dnelson/prometric/storm_ingestion/apache-storm-0.9.3 -e 'bin/storm ui' \
--tab -t 'storm supervisor' --working-directory=/home/dnelson/prometric/storm_ingestion/apache-storm-0.9.3 -e 'bin/storm supervisor' \
--tab -t 'namenode' --working-directory=/home/dnelson/prometric/storm_ingestion/hadoop-2.6.0 -e 'bin/hdfs namenode' \
--tab -t 'datanode' --working-directory=/home/dnelson/prometric/storm_ingestion/hadoop-2.6.0 -e 'bin/hdfs datanode' 

# Topology Deployment
## Build
    mvn clean package;
## Deploy hdfs topology 
    ../apache-storm-0.9.3/bin/storm jar target/prometric-storm-1.0-SNAPSHOT.jar hortonworks.prometric.kafka.hdfs.CrfHdfsTopology; complete
## Kill hdfs topology
    ../apache-storm-0.9.3/bin/storm kill crf-hdfs-topology -w 1;
## Deploy hive topology 
    ../apache-storm-0.9.3/bin/storm jar target/prometric-storm-1.0-SNAPSHOT.jar hortonworks.prometric.kafka.hive.CrfHiveTopology; complete
## Kill hive topology
    ../apache-storm-0.9.3/bin/storm kill crf-hive-topology -w 1;




crf-hive-topology

export HADOOP_CLASSPATH=/home/dnelson/prometric/storm_ingestion/hadoop-2.6.0/share/hadoop/yarn/test/hadoop-yarn-server-tests-2.6.0-tests.jar
./bin/hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-tests.jar minicluster -rmport 3000 -jhsport 3001


 ../apache-storm-0.9.3/bin/storm kill CRF-topology -w 1; rm -rf /tmp/zookeeper;


org.apache.hadoop.ipc.RemoteException: File could only be replicated to 0 nodes instead of minReplication.  There are datanode running and node are excluded in this operation.



https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-InstallingHivefromaStableRelease


export PATH=$PATH:/home/dnelson/prometric/storm_ingestion/apache-hive-0.14.0-bin:/home/dnelson/prometric/storm_ingestion/hadoop-2.6.0


export HIVE_HOME=/home/dnelson/prometric/storm_ingestion/apache-hive-0.14.0-bin