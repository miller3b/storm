package hortonworks.prometric.kafka.hive;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.StringScheme;

import hortonworks.prometric.kafka.hive.XmlBolt;

import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;

public class CrfHiveTopology {

  private static final String CRF_SPOUT_ID = "crf-hive-kafka-spout";
  private static final String XML_BOLT_ID = "crf-xml-bolt";
  private static final String HIVE_BOLT_ID = "crf-hive-bolt";
  private static final String TOPOLOGY_NAME = "crf-hive-topology";

  public static void main(String[] args) throws Exception {
    int numSpoutExecutors = 1;
    
    KafkaSpout crfSpout = buildCRFArtifactSpout();
    XmlBolt xmlBolt = new XmlBolt();
    HiveBolt hiveBolt = buildHiveBolt();
    TopologyBuilder builder = new TopologyBuilder();
    
    builder.setSpout(CRF_SPOUT_ID, crfSpout, numSpoutExecutors);
    builder.setBolt(XML_BOLT_ID, hiveBolt).shuffleGrouping(CRF_SPOUT_ID);
    builder.setBolt(HIVE_BOLT_ID, hiveBolt).shuffleGrouping(CRF_SPOUT_ID);

    Config cfg = new Config();    
    StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
  }

  private static HiveBolt buildHiveBolt() {
    DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper();

    String metaStoreURI = "thrift://bil-hdp-app-01.prometric.qc2:9083";
    String dbName = "crf";
    String tblName = "test";

    HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper);
    // Configuration for when running in secure mode
     hiveOptions.withKerberosPrinicipal("hive/bil-hdp-app-01.prometric.qc2@HADOOP.PROMETRIC.QC2")
                .withKerberosKeytab("/etc/security/keytab/hive.service.keytab");
    HiveBolt hiveBolt = new HiveBolt(hiveOptions);
    return hiveBolt;
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
