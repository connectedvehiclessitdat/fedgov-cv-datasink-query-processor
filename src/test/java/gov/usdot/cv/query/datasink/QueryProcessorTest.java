package gov.usdot.cv.query.datasink;

import static org.junit.Assert.assertTrue;
import gov.usdot.asn1.generated.j2735.J2735;
import gov.usdot.asn1.generated.j2735.semi.AdvisorySituationDataDistribution;
import gov.usdot.asn1.j2735.J2735Util;
import gov.usdot.cv.common.util.UnitTestHelper;
import gov.usdot.cv.query.datasink.receiver.AdvisoryBroadcastReceiver;
import gov.usdot.cv.resources.PrivateTestResourceLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONObject;

import org.apache.activemq.broker.BrokerService;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.oss.asn1.AbstractData;
import com.oss.asn1.Coder;

/**
 * MongoDB doesn't offer a embedded mode function, so these unit tests were written to uses a
 * MongoDB instance running on your localhost.
 */
public class QueryProcessorTest {
	
	static
	{
		UnitTestHelper.initLog4j(Level.INFO);
		Properties testProperties = System.getProperties();
		if (testProperties.getProperty("RTWS_CONFIG_DIR") == null) {
			try {
				testProperties.load(
						PrivateTestResourceLoader.getFileAsStream(
								"@properties/datasink-query-processor-filtering.properties@"));

				System.setProperties(testProperties);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static final String REMOTE_JMS_BROKER_NAME = "EXTERNAL_JMS_BROKER";
	private static final String REMOTE_JMS_CONNECTOR_URL = "tcp://localhost:61619";
	
	private static BrokerService externalJMSBroker;
	
	private static Coder coder;
	
	private static ArrayBlockingQueue<ByteBuffer> queue = new ArrayBlockingQueue<ByteBuffer>(25);
	private static Thread receiver_t;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.out.println("Starting external jms broker service ...");
		externalJMSBroker = new BrokerService();
		externalJMSBroker.setBrokerName(REMOTE_JMS_BROKER_NAME);
		externalJMSBroker.addConnector(REMOTE_JMS_CONNECTOR_URL);
		externalJMSBroker.setPersistent(false);
		externalJMSBroker.setUseJmx(false);
		externalJMSBroker.start();
		
		System.setProperty("messaging.external.connection.url", REMOTE_JMS_CONNECTOR_URL);
		
		System.out.println("Intializing J2735 coder ...");
		J2735.initialize();
		coder = J2735.getPERUnalignedCoder();
		
		System.out.println("Starting advisory broadcast receiver ...");
		receiver_t = new Thread(new AdvisoryBroadcastReceiver(queue, 46751));
		receiver_t.start();
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		System.out.println("Stopping external jms broker service ...");
		externalJMSBroker.stop();
	}
	
	@Test @Ignore
	public void testQueryUnitedStatesRegion() throws Exception {
		System.out.println(">>> Running testQueryUnitedStatesRegion() ...");
		
		JSONObject request = buildSampleQueryRequest();
		
		QueryProcessor processor = new QueryProcessor();
		processor.setDatabaseName("cvdb");
		processor.setMongoServerHost("localhost");
		processor.setMongoServerPort(27017);
		processor.setAutoConnectRetry(true);
		processor.setConnectTimeoutMs(3000);
		processor.setBundleForwarderHost("127.0.0.1");
		processor.setBundleForwarderPort(46751);
		processor.setBroadcastInstructionsFieldName("broadcastInstructions");
		processor.setReceiptTopicName("cv.receipts");
		processor.setDbAdvSitDataCollection("travelerInformation");
		processor.setDbIntersectionSitDataCollection("intersectionSitData");
		processor.setGeospatialFieldName("region");
		processor.initialize();
		processor.process(request);
		
		try { Thread.sleep(5); } catch(InterruptedException ie) {}
		
		List<ByteBuffer> responses = collectResponses(1);
		assertTrue("Expecting 1 response(s) but got '" + responses.size() + "'.", responses.size() == 1);
		
		AbstractData bundle = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a AdvisorySituationDataDistribution message.", bundle instanceof AdvisorySituationDataDistribution);
		
		processor.dispose();
	}
	
	@Test @Ignore
	public void testQueryIntersectsSoutheastMichiganRegion() throws Exception {
		System.out.println(">>> Running testQuerySoutheastMichiganRegion() ...");
		
		JSONObject request = buildSampleQueryRequest();
		request.getJSONObject("nwPos").put("lat", 42.0);
		request.getJSONObject("nwPos").put("lon", -86.0);
		request.getJSONObject("sePos").put("lat", 40.0);
		request.getJSONObject("sePos").put("lon", -83.0);
		
		QueryProcessor processor = new QueryProcessor();
		processor.setDatabaseName("cvdb");
		processor.setMongoServerHost("localhost");
		processor.setMongoServerPort(27017);
		processor.setAutoConnectRetry(true);
		processor.setConnectTimeoutMs(3000);
		processor.setBundleForwarderHost("127.0.0.1");
		processor.setBundleForwarderPort(46751);
		processor.setBroadcastInstructionsFieldName("broadcastInstructions");
		processor.setReceiptTopicName("cv.receipts");
		processor.setDbAdvSitDataCollection("travelerInformation");
		processor.setDbIntersectionSitDataCollection("intersectionSitData");
		processor.setGeospatialFieldName("region");
		processor.initialize();
		processor.process(request);
		
		try { Thread.sleep(5); } catch(InterruptedException ie) {}
		
		List<ByteBuffer> responses = collectResponses(1);
		assertTrue("Expecting 1 response(s) but got '" + responses.size() + "'.", responses.size() == 1);
		
		AbstractData bundle = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a AdvisorySituationDataDistribution message.", bundle instanceof AdvisorySituationDataDistribution);
		
		processor.dispose();
	}
	
	@Test @Ignore
	public void testQueryOutsideSoutheastMichiganRegion() throws Exception {
		System.out.println(">>> Running testQueryOutsideSoutheastMichiganRegion() ...");
		
		JSONObject request = buildSampleQueryRequest();
		request.getJSONObject("nwPos").put("lat", 49.0);
		request.getJSONObject("nwPos").put("lon", -81.0);
		request.getJSONObject("sePos").put("lat", 44.0);
		request.getJSONObject("sePos").put("lon", -78.0);
		
		QueryProcessor processor = new QueryProcessor();
		processor.setDatabaseName("cvdb");
		processor.setMongoServerHost("localhost");
		processor.setMongoServerPort(27017);
		processor.setAutoConnectRetry(true);
		processor.setConnectTimeoutMs(3000);
		processor.setBundleForwarderHost("127.0.0.1");
		processor.setBundleForwarderPort(46751);
		processor.setBroadcastInstructionsFieldName("broadcastInstructions");
		processor.setReceiptTopicName("cv.receipts");
		processor.setDbAdvSitDataCollection("travelerInformation");
		processor.setDbIntersectionSitDataCollection("intersectionSitData");
		processor.setGeospatialFieldName("region");
		processor.initialize();
		processor.process(request);
		
		try { Thread.sleep(5); } catch(InterruptedException ie) {}
		
		List<ByteBuffer> responses = collectResponses(3);
		assertTrue("Expecting 0 response(s) but got '" + responses.size() + "'.", responses.size() == 1);
		
		processor.dispose();
	}
	
	private JSONObject buildSampleQueryRequest() {
		JSONObject request = new JSONObject();
		request.put("receiptId", UUID.randomUUID().toString());
		request.put("dialogId", 157);
		request.put("sequenceId", 3);
		request.put("groupId", 0);	
		request.put("requestId", 1000);
		request.put("targetHost", "127.0.0.1");
		request.put("targetPort", 46751);
		
		JSONObject nwPos = new JSONObject();
		nwPos.put("lat", 48.374353);
		nwPos.put("lon", -131.643968);
		request.put("nwPos", nwPos);
		
		JSONObject sePos = new JSONObject();
		sePos.put("lat", 24.156250);
		sePos.put("lon", -72.347240);
		request.put("sePos", sePos);
		
		return request;
	}
	
	private List<ByteBuffer> collectResponses(int expected) {
		ArrayList<ByteBuffer> responses = new ArrayList<ByteBuffer>();
		while (expected > 0) try {
			ByteBuffer response = queue.poll(100, TimeUnit.MILLISECONDS);
			if (response != null) responses.add(response);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			expected--;
		}
		return responses;
	}
	
}