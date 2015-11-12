package hortonworks.prometric.kafka.hive;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.StringScheme;
import hortonworks.prometric.kafka.hive.XmlBolt;

import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;


import java.io.IOException;
//import hortonworks.prometric.kafka.hive.TestSpout;
//export HADOOP_OPTS ="$HADOOP_OPT -Dsun.security.krb5.debug=true";
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class CrfHiveTopology {
	
	public CrfHiveTopology()
	{
		configLogger(logger);
	}
	
	private static Logger logger = Logger.getLogger(CrfHiveTopology.class.getName());
	private static String logFilePath = "/var/log/storm/Debugging.log"; // TODO: Get Log File Path

	private static final String CRF_SPOUT_ID = "crf-hive-kafka-spout";
	private static final String XML_BOLT_ID = "crf-xml-bolt";
	private static final String HIVE_BOLT_ID = "crf-hive-bolt";
	private static final String TOPOLOGY_NAME = "crf-hive-topology";
	private static final String[] colNames = { "confirmationnumber",
			"clientcode", "programcode", "sitecode", "examcode",
			"examformname", "eventdate", "processedtimestamp", "crf" };
	private static final String Stormusers = "mmcglone@PROMETRIC.QC2, ben.miller@PROMETRIC.QC2";
	public static void main(String[] args) throws Exception {
		int numSpoutExecutors = 1;
		
		for(int i = 0; i < args.length; i++)
		{
			logger.info("areid - args[" + i + "]: " + args[i]);
		}

		KafkaSpout crfSpout = buildCRFArtifactSpout();
		XmlBolt xmlBolt = new XmlBolt();
		HiveBolt hiveBolt = buildHiveBolt();
		TopologyBuilder builder = new TopologyBuilder();

		// builder.setSpout(CRF_SPOUT_ID, new TestSpout(), numSpoutExecutors);
		builder.setSpout(CRF_SPOUT_ID, crfSpout, numSpoutExecutors);
		builder.setBolt(XML_BOLT_ID, xmlBolt).shuffleGrouping(CRF_SPOUT_ID);
		builder.setBolt(HIVE_BOLT_ID, hiveBolt).shuffleGrouping(XML_BOLT_ID); //, new Fields("confirmationnumber","clientcode", "programcode", 
		//"sitecode", "examcode","examformname", "eventdate", "processedtimestamp", "crf"));

		Config cfg = new Config(); 
//		cfg.put("topology.users", Stormusers);
		cfg.put(Config.TOPOLOGY_DEBUG, true);		
		StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());

		//Config cfg = new Config();
		//cfg.put(Config.TOPOLOGY_DEBUG, true);
		//StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
	}

	private static HiveBolt buildHiveBolt() {

		DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
				.withColumnFields(new Fields(colNames));
		String metaStoreURI = "thrift://bil-hdp-app-01.prometric.qc2:9083";
		String dbName = "results_db";
		String tblName = "resultsartifact";

		// HiveOptions hiveOptions = new
		// HiveOptions(metaStoreURI,dbName,tblName,mapper);
		// Configuration for when running in secure mode
		// hiveOptions.withKerberosPrincipal("storm@HADOOP.PROMETRIC.QC2")
		// .withKerberosKeytab("/home/storm/storm.service.keytab");

		HiveOptions hiveOptions = new HiveOptions(metaStoreURI, dbName,
				tblName, mapper);
		hiveOptions.withTxnsPerBatch(10);
		hiveOptions.withBatchSize(10);
		hiveOptions.withIdleTimeout(10);
		hiveOptions.withKerberosKeytab("/etc/security/keytabs/storm.headless.keytab");
		hiveOptions.withKerberosPrincipal("storm@HADOOP.PROMETRIC.QC2");

		HiveBolt hiveBolt = new HiveBolt(hiveOptions);
		return hiveBolt;
	}

	private static KafkaSpout buildCRFArtifactSpout() {
		String topic = "eom_bil";
		String zkRoot = "/crf-hive-spout";
		String zkSpoutId = "crf-hive-spout";

		// TODO make into an array or other iterable
		ZkHosts zkHost1 = new ZkHosts("bil-hdp-app-05:2181");
		// ZkHosts zkHost1 = new ZkHosts("127.0.0.1:2181");
		// ZkHosts zkHost2 = new ZkHosts("bil-hdp-app-06:2181");
		// ZkHosts zkHost3 = new ZkHosts("bil-hdp-app-07:2181");

		SpoutConfig spoutCfg = new SpoutConfig(zkHost1, topic, zkRoot,
				zkSpoutId);
		spoutCfg.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutCfg.forceFromStart = true;
		KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
		return kafkaSpout;
	}
	
	private static void configLogger(Logger logger) {
		FileHandler fh;
		try {

			// This block configures the logger with handler and formatter
			fh = new FileHandler(logFilePath);
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);

		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
