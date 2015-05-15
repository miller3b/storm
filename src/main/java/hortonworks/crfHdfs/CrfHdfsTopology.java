package hortonworks.prometric.kafka.hdfs;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.KafkaConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.StringScheme;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.DefaultSequenceFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;

public class CrfHdfsTopology {

  private static final String CRF_SPOUT_ID = "crf-hdfs-kafka-spout";
  private static final String RAW_BOLT_ID = "crf-hdfs-bolt";
  private static final String TOPOLOGY_NAME = "crf-hdfs-topology";

  public static void main(String[] args) throws Exception {
    int numSpoutExecutors = 1;
    
    KafkaSpout crfSpout = buildCRFArtifactSpout();
    HdfsBolt rawBolt = buildRawHdfsBolt();

    TopologyBuilder builder = new TopologyBuilder();
    
    builder.setSpout(CRF_SPOUT_ID, crfSpout, numSpoutExecutors);
    builder.setBolt(RAW_BOLT_ID, rawBolt).shuffleGrouping(CRF_SPOUT_ID);

    Config cfg = new Config();
    // Configuration for when running in secure mode
    // cfg.put("hdfs.keytab.file","/etc/security/keytabs/storm.service.keytab");
    // cfg.put("hdfs.kerberos.principal","storm@HADOOP.PROMETRIC.QC2");
    // cfg.put("topology.auto-credentials", "org.apache.storm.hdfs.common.security.AutoHDFS");

    StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
  }

  private static HdfsBolt buildRawHdfsBolt() {
    // RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");
    RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

    //Synchronize data buffer with the filesystem every 1000 tuples
    SyncPolicy syncPolicy = new CountSyncPolicy(1);

    // Rotate data files when they reach five MB
    FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

    // Use default, Storm-generated file names
    FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp");
    

   HdfsBolt bolt = new HdfsBolt()
        .withFsUrl("hdfs://bilha")
        .withFileNameFormat(fileNameFormat)
        .withRecordFormat(format)
        .withRotationPolicy(rotationPolicy)
        .withSyncPolicy(syncPolicy);
    return bolt;
  }

  private static KafkaSpout buildCRFArtifactSpout() {
    String topic = "eom_bil";
    String zkRoot = "/crf-hive-spout";
    String zkSpoutId = "crf-hive-spout";
    
    // TODO make into an array or other iterable
    ZkHosts zkHost1 = new ZkHosts("bil-hdp-app-05:2181");
    // ZkHosts zkHost2 = new ZkHosts("bil-hdp-app-06:2181");
    // ZkHosts zkHost3 = new ZkHosts("bil-hdp-app-07:2181");

    SpoutConfig spoutCfg = new SpoutConfig(zkHost1, topic, zkRoot, zkSpoutId);
    spoutCfg.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutCfg.forceFromStart = true;
    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
    return kafkaSpout;
  }
}
