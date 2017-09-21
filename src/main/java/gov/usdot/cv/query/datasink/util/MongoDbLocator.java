package gov.usdot.cv.query.datasink.util;

import gov.usdot.cv.query.datasink.model.Item;
import gov.usdot.cv.query.datasink.model.ProcessFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.UnknownHostException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.deleidos.rtws.commons.cloud.platform.StorageInterface;
import com.deleidos.rtws.commons.cloud.util.InterfaceConfig;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public class MongoDbLocator {
	private static final Logger logger = Logger.getLogger(MongoDbLocator.class);
	
	private static final String MONGODB_GROUP_NAME = "mongodb.standalone";
	private static final int MONGODB_INSTANCE_NUNBER = 1;
	
	private MongoDbLocator() {}
	
	public static synchronized String getHostname(
			String processFilePath, 
			int mongoServerPort) {
		while (true) {
			ProcessFile pf = null;
			try {
				InputStream is = null;
				if (processFilePath.startsWith("s3://")) {
					logger.info(String.format("Loading %s file from remote storage ...", processFilePath));
					StorageInterface service = InterfaceConfig.getInstance().getStorageInterface();
				
					String processFileWithScheme = processFilePath.replaceFirst("s3:\\/\\/", "");
					int idx = processFileWithScheme.indexOf("/");
					String bucketName = processFileWithScheme.substring(0, idx);
					String fileKey = processFileWithScheme.substring(idx + 1);
				
					is = service.getFile(bucketName, fileKey);
				} else {
					logger.info(String.format("Loading %s file from local storage ...", processFilePath));
					is = new FileInputStream(new File(processFilePath));
				}
				
				logger.info("Parsing processes.xml file ...");
				if (is != null) try {
					pf = new ProcessFile(is);
				} finally {
					if (is != null) {
						try { is.close(); } catch (Exception ignore) {}
					}
				}
			
				if (pf != null) {
					Item mongodb = pf.getItem(MONGODB_INSTANCE_NUNBER, MONGODB_GROUP_NAME);
					String hostname = mongodb.getPrivateIpAddress();
					logger.debug(String.format("Obtained MongoDB IP address '%s' from processes.xml file.", hostname));
					if (! StringUtils.isEmpty(hostname)) {
						logger.debug(String.format("Testing MongoDB connectivity '%s:%s'.", hostname, mongoServerPort));
						testConnectivity(hostname, mongoServerPort);
						return hostname;
					}
				}
			} catch (Exception ex) {
				logger.error(String.format("Failed to locate MongoDb hostname [processFile=%s, port=%s].", processFilePath, mongoServerPort), ex);
			}
			
			try { Thread.sleep(60 * 1000); } catch (InterruptedException ignore) {}
		}
	}
	
	@SuppressWarnings( "deprecation" )
	private static synchronized void testConnectivity(
			String hostname, 
			int port) throws MongoException, UnknownHostException {
		Mongo client = new Mongo(new ServerAddress(hostname, port));
		client.getDatabaseNames();
	}
}