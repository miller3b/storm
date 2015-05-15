package hortonworks.prometric.kafka.hive;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.*;

public class XmlBolt extends BaseBasicBolt {

  public static Document stringToDom(String xmlSource) 
	  throws SAXException, ParserConfigurationException, IOException {

	DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	DocumentBuilder builder = factory.newDocumentBuilder();
	return builder.parse(new InputSource(new StringReader(xmlSource)));
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
  	//TODO make this try / catch block useful
  	try 
  	{
    	System.out.println(tuple);

		Document xml = stringToDom(tuple.getString(0));    

		System.out.println(xml);
	}
	catch (Exception e)
	{
		System.out.println(e);
	}
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}