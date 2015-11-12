package hortonworks.prometric.kafka.hive;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.xml.parsers.*;

import org.w3c.dom.*;
import org.xml.sax.*;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class XmlBolt extends BaseBasicBolt {

	/**
	 * Default Serial Version Id
	 */
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(XmlBolt.class.getName());

	private static String logFilePath = "/var/log/storm/Debugging.log";

	public XmlBolt() {
		configLogger();
	}

	public static Document stringToDom(String xmlSource) throws SAXException,
			ParserConfigurationException, IOException {

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		return builder.parse(new InputSource(new StringReader(xmlSource)));
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			XPathFactory xpathFactory = XPathFactory.newInstance();
			XPath xpath = xpathFactory.newXPath();
			String crf = tuple.getString(0);
			Document xml = stringToDom(crf);

			crf = crf;
			String programcode = (String) xpath.compile(xPathProgramCode())
					.evaluate(xml, XPathConstants.STRING);
			String examcode = (String) xpath.compile(xPathExamCode()).evaluate(
					xml, XPathConstants.STRING);
			String sitecode = (String) xpath.compile(xPathSiteCode()).evaluate(
					xml, XPathConstants.STRING);
			String examformname = (String) xpath.compile(xPathExamFormName())
					.evaluate(xml, XPathConstants.STRING);
			String eventdate = (String) xpath.compile(xPathDate()).evaluate(
					xml, XPathConstants.STRING);
//			String examformname = "B28411";
//			String eventdate = "20150601";
			String clientcode = (String) xpath.compile(xPathClientCode())
					.evaluate(xml, XPathConstants.STRING);
			DateTime processtimestamp = new DateTime(DateTimeZone.UTC);
			String confirmationnumber = (String) xpath.compile(
					xPathConfirmationNumber()).evaluate(xml,
					XPathConstants.STRING);
			collector.emit(new Values(confirmationnumber, clientcode,
					programcode, eventdate, sitecode, examcode, examformname,
					processtimestamp.toString(), crf));
			
			logger.info("programcode :" + programcode + 
					" | examcode :" + examcode + 
					" | sitecode :" + sitecode + 
					" | examformname :" + examformname + 
					" | eventdate :" + eventdate + 
					" | clientcode :" + clientcode + 
					" | processtimestamp :" + processtimestamp.toString() + 
					" | confirmationnumber :" + confirmationnumber +
					" | crf :" + crf);

		} catch (Exception e) {
			System.out.println(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("confirmationnumber", "clientcode",
				"programcode", "eventdate", "sitecode", "examcode",
				"examformname", "processedtimestamp", "crf"));
	}

	private String xPathProgramCode() {
		return "//programcode/text()";
	}

	private String xPathConfirmationNumber() {
		return "//confirmationnumber/text()";
	}

	private String xPathSiteCode() {
		return "//sitecode/text()";
	}

	private String xPathExamCode() {
		return "//examcode/text()";
	}

	private String xPathExamFormName() {
		return "//examformname/text()";
	}

	private String xPathDate() {
		return "//eventdatetime/text()";
	}

	private String xPathClientCode() {
		return "//clientcode/text()";
	}

	private static void configLogger() {
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
