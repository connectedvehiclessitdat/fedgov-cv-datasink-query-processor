package gov.usdot.cv.query.datasink;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import gov.usdot.cv.common.util.UnitTestHelper;
import gov.usdot.cv.query.datasink.model.Item;
import gov.usdot.cv.query.datasink.model.ProcessFile;

import org.apache.log4j.Level;
import org.junit.Ignore;
import org.junit.Test;

import com.deleidos.rtws.commons.cloud.platform.jetset.JetSetConnectionFactory;
import com.deleidos.rtws.commons.cloud.platform.jetset.JetSetStorageService;

public class ProcessFileTest {
	
	static
	{
		UnitTestHelper.initLog4j(Level.INFO);
		if (System.getProperty("RTWS_CONFIG_DIR") == null) {
			System.setProperty("RTWS_CONFIG_DIR", System.getProperty("basedir", ".") + "/src/test/resources");
		}
	}
	
	@Ignore
	public void testLocalProcessFile() throws Exception {
		File f = new File(System.getProperty("basedir", ".") + "/src/test/resources/sdw-processes.xml");
		ProcessFile pf = new ProcessFile(new FileInputStream(f));
		Item mongodb1 = pf.getItem(1, "mongodb.standalone");
		
		assertNotNull(mongodb1);
		assertTrue(mongodb1.getNumber() == 1);
		assertTrue(mongodb1.getAllocateInternetAddress() == false);
		assertTrue(mongodb1.getGroup().equals("mongodb.standalone"));
		assertTrue(mongodb1.getPrivateIpAddress().equals("10.118.15.133"));
		assertNotNull(mongodb1.getVolumes());
		assertTrue(mongodb1.getVolumes().size() == 2);
		
		Item ingest1 = pf.getItem(1, "ingest.all");
		
		assertNotNull(ingest1);
		assertTrue(ingest1.getNumber() == 1);
		assertTrue(ingest1.getAllocateInternetAddress() == false);
		assertTrue(ingest1.getId().equals("i-d6980e85"));
		assertTrue(ingest1.getGroup().equals("ingest.all"));
		assertTrue(ingest1.getPublicDnsName().equals("ec2-50-19-75-230.compute-1.amazonaws.com"));
		assertTrue(ingest1.getPublicIpAddress().equals("50.19.75.230"));
		assertNotNull(ingest1.getVolumes());
		assertTrue(ingest1.getVolumes().size() == 0);
		
		assertTrue(pf.getAll().size() == 9);
	}
	
	@Test @Ignore
	public void testS3ProcessFile() throws Exception {
		String secretKey = "SECRET KEY";
		String accessKey = "ACCESS KEY";
		
		JetSetConnectionFactory factory = new JetSetConnectionFactory();
		factory.setCredentials(accessKey, secretKey);
		factory.setStorageEndpoint("s3.amazonaws.com");
		factory.setStoragePortNumber("80");
		
		JetSetStorageService service = new JetSetStorageService();
		service.setConnectionFactory(factory);
		
		String uri = "s3://rtws.account.391/process/dev-sdw.cv-dev.aws-dev.deleidos.com/processes.xml";
		String path = uri.replaceFirst("s3:\\/\\/", "");
		int idx = path.indexOf("/");
		String bucketName = path.substring(0, idx);
		String fileKey = path.substring(idx + 1);
		
		InputStream is = service.getFile(bucketName, fileKey);
		ProcessFile pf = new ProcessFile(is);
		
		Item mongodb1 = pf.getItem(1, "mongodb.standalone");
		assertNotNull(mongodb1);
		
		Item ingest1 = pf.getItem(1, "ingest.all");
		assertNotNull(ingest1);
	}
	
}