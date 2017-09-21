package gov.usdot.cv.query.datasink;

import gov.usdot.asn1.generated.j2735.J2735;
import gov.usdot.cv.common.database.mongodb.MongoOptionsBuilder;
import gov.usdot.cv.common.database.mongodb.criteria.DateRange;
import gov.usdot.cv.common.database.mongodb.dao.QueryAdvSitDataDao;
import gov.usdot.cv.common.database.mongodb.dao.QueryIntersectionSitDataDao;
import gov.usdot.cv.common.database.mongodb.geospatial.Coordinates;
import gov.usdot.cv.common.database.mongodb.geospatial.Geometry;
import gov.usdot.cv.common.database.mongodb.geospatial.Point;
import gov.usdot.cv.common.dialog.Receipt;
import gov.usdot.cv.common.dialog.ReceiptSender;
import gov.usdot.cv.common.inet.InetPacketSender;
import gov.usdot.cv.common.inet.InetPoint;
import gov.usdot.cv.common.model.BoundingBox;
import gov.usdot.cv.common.util.PropertyLocator;
import gov.usdot.cv.common.util.Syslogger;
import gov.usdot.cv.query.datasink.exception.QueryProcessorException;
import gov.usdot.cv.query.datasink.model.DataModel;
import gov.usdot.cv.query.datasink.processor.IntersectionSitDataResultProcessor;
import gov.usdot.cv.query.datasink.processor.RsuAdvSitDataResultProcessor;
import gov.usdot.cv.query.datasink.util.MongoDbLocator;
import gov.usdot.cv.query.datasink.validator.BoundingBoxValidator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.deleidos.rtws.commons.exception.InitializationException;
import com.deleidos.rtws.core.framework.Description;
import com.deleidos.rtws.core.framework.SystemConfigured;
import com.deleidos.rtws.core.framework.UserConfigured;
import com.deleidos.rtws.core.framework.processor.AbstractDataSink;
import com.mongodb.DBObject;
import com.oss.asn1.Coder;

@Description("Process traveler information query request.")
public class QueryProcessor extends AbstractDataSink {
	
	private final static String SYS_LOG_ID = "UDP QueryProcessor";
	public static final String CREATED_AT_FIELD		= "createdAt";
	
	private final Logger logger = Logger.getLogger(getClass());
	
	private String 			databaseName;
	private String			processFilePath;
	private String 			mongoServerHost;
	private int    			mongoServerPort;
	private boolean 		autoConnectRetry = true;
	private int 			connectTimeoutMs = 0;
	private boolean			forwardAll = false;
	private String 			bundleForwarderHost;
	private int    			bundleForwarderPort = -1;
	private String			receiptJmsHost;
	private int				receiptJmsPort = -1;
	private String 			topicName;
	private String 			broadcastInstructionsFieldName;
	private String			geospatialFieldName;
	
	private Coder 			coder;
	private String			dbAdvSitDataCollection;
	private String			dbIntersectionSitDataCollection;
	
	private QueryAdvSitDataDao 			advSitDataDao;
	private QueryIntersectionSitDataDao intersectionSitDataDao;
	
	private ReceiptSender 		receiptSender;
	
	private RsuAdvSitDataResultProcessor 		advSitDataResultProcessor;
	private IntersectionSitDataResultProcessor 	intersectionSitDataResultProcessor;

	@Override
	@SystemConfigured(value = "Traveler Information Query Processor")
	public void setName(String name) {
		super.setName(name);
	}
	
	@Override
	@SystemConfigured(value = "cvquery")
	public void setShortname(String shortname) {
		super.setShortname(shortname);
	}
	
	@UserConfigured(value="cvdb", description="The database name to use to store processed records.")
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	public String getDatabaseName() {
		return this.databaseName;
	}
	
	@UserConfigured(
		value = "s3://bucket-name/path/processes.xml", 
		description = "Absolute path of the pubsub server keystore file.")
	public void setWarehouseProcessFilePath(String processFilePath) {
		this.processFilePath = processFilePath;
	}
	
	public String getWarehouseProcessFilePath() {
		return this.processFilePath;
	}
	
	@UserConfigured(
		value= "127.0.0.1", 
		description="The MongoDB server hostname.", 
		flexValidator={"StringValidator minLength=2 maxLength=1024"})
	public void setMongoServerHost(String mongoServerHost) {
		if (mongoServerHost != null) {
			this.mongoServerHost = mongoServerHost.trim();
		}
	}
		
	@NotNull
	public String getMongoServerHost() {
		return this.mongoServerHost;
	}
		
	@UserConfigured(
		value = "27017", 
		description = "The MongoDB server port number.", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setMongoServerPort(int mongoServerPort) {
		this.mongoServerPort = mongoServerPort;
	}
		
	@Min(0)
	@Max(65535)
	public int getMongoServerPort() {
		return this.mongoServerPort;
	}
	
	@UserConfigured(
		value = "true",
		description = "MongoDB client auto connect retry flag.",
		flexValidator = {"RegExpValidator expression=true|false"})
	public void setAutoConnectRetry(boolean autoConnectRetry) {
		this.autoConnectRetry = autoConnectRetry;
	}
	
	@NotNull
	public boolean getAutoConnectRetry() {
		return this.autoConnectRetry;
	}
		
	@UserConfigured(
		value = "3000",
		description = "Time (in milliseconds) to wait for a successful connection.",
		flexValidator = {"NumberValidator minValue=0 maxValue=" + Integer.MAX_VALUE})
	public void setConnectTimeoutMs(int connectTimeoutMs) {
		this.connectTimeoutMs = connectTimeoutMs;
	}
	
	@NotNull
	public int getConnectTimeoutMs() {
		return this.connectTimeoutMs;
	}
	
	@UserConfigured(
		value = "true",
		description = "Flag indicating if all responses will be forwarded or not.",
		flexValidator = {"RegExpValidator expression=true|false"})
	public void setForwardAll(boolean forwardAll) {
		this.forwardAll = forwardAll;
	}
	
	@NotNull
	public boolean getForwardAll() {
		return this.forwardAll;
	}
	
	@UserConfigured(
		value= "127.0.0.1", 
		description="The bundle forwarder host.", 
		flexValidator={"StringValidator minLength=2 maxLength=1024"})
	public void setBundleForwarderHost(String bundleForwarderHost) {
		this.bundleForwarderHost = bundleForwarderHost;
	}
			
	@NotNull
	public String getBundleForwarderHost() {
		return this.bundleForwarderHost;
	}
			
	@UserConfigured(
		value = "46761", 
		description = "The bundle forwarder port number.", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setBundleForwarderPort(int bundleForwarderPort) {
		this.bundleForwarderPort = bundleForwarderPort;
	}
			
	@Min(0)
	@Max(65535)
	public int getBundleForwarderPort() {
		return this.bundleForwarderPort;
	}
	
	@UserConfigured(
		value= "", 
		description="The receipt jms server hostname.", 
		flexValidator={"StringValidator minLength=0 maxLength=1024"})
	public void setReceiptJmsHost(String receiptJmsHost) {
		this.receiptJmsHost = receiptJmsHost;
	}
	
	@NotNull
	public String getReceiptJmsHost() {
		return this.receiptJmsHost;
	}
	
	@UserConfigured(
		value = "61617", 
		description = "The receipt jms server port.", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setReceiptJmsPort(int receiptJmsPort) {
		this.receiptJmsPort = receiptJmsPort;
	}
	
	@Min(0)
	@Max(65535)
	public int getReceiptJmsPort() {
		return this.receiptJmsPort;
	}
	
	@UserConfigured(
		value = "cv.receipts",
		description = "The jms topic to place receipts.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setReceiptTopicName(String topicName) {
		this.topicName = topicName;
	}
	
	@UserConfigured(
		value = "travelerInformation",
		description = "Name of the advisory situation data collection.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setDbAdvSitDataCollection(String dbAdvSitDataCollection) {
		this.dbAdvSitDataCollection = dbAdvSitDataCollection;
	}
	
	@UserConfigured(
		value = "intersectionSitData",
		description = "Name of the intersection situation data collection.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setDbIntersectionSitDataCollection(String dbIntersectionSitDataCollection) {
		this.dbIntersectionSitDataCollection = dbIntersectionSitDataCollection;
	}
	
	@UserConfigured(
		value = "broadcastInstructions",
		description = "Name of the field that contains the broadcast instructions.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setBroadcastInstructionsFieldName(String broadcastInstructionsFieldName) {
		this.broadcastInstructionsFieldName = broadcastInstructionsFieldName;
	}
	
	@UserConfigured(
		value = "region",
		description = "Name of the field to perform geospatial query.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setGeospatialFieldName(String geospatialFieldName) {
		this.geospatialFieldName = geospatialFieldName;
	}
	
	public void initialize() throws InitializationException {
		try {
			logger.info("Setting MongoDB hostname ...");
			if (this.processFilePath != null &&
				(StringUtils.isEmpty(this.mongoServerHost) || 
				this.mongoServerHost.equals("127.0.0.1") || 
				this.mongoServerHost.equals("localhost"))) {
				this.mongoServerHost = MongoDbLocator.getHostname(this.processFilePath, this.mongoServerPort);
			} else {
				String domain = PropertyLocator.getString("RTWS_DOMAIN", null);
				this.mongoServerHost = (domain == null) ? "localhost" : String.format(mongoServerHost, domain);
			}
			
			logger.info("Constructing J2735 BER coder ...");
			J2735.initialize();
			coder = J2735.getPERUnalignedCoder();
			
			logger.info("Constructing MongoDB data access object ...");
			MongoOptionsBuilder optionsBuilder = new MongoOptionsBuilder();
			optionsBuilder.setAutoConnectRetry(this.autoConnectRetry).setConnectTimeoutMs(this.connectTimeoutMs);
			
			logger.info(String.format("Setting MongoDB host to '%s' and port to '%s' ...", this.mongoServerHost, this.mongoServerPort));
			this.advSitDataDao = QueryAdvSitDataDao.newInstance(
				this.mongoServerHost, 
				this.mongoServerPort, 
				optionsBuilder.build(),
				this.databaseName);
			this.intersectionSitDataDao = QueryIntersectionSitDataDao.newInstance(
				this.mongoServerHost, 
				this.mongoServerPort, 
				optionsBuilder.build(),
				this.databaseName);
			
			InetPoint forwarderPoint = null;
			if (this.bundleForwarderHost != null && this.bundleForwarderPort != 0) {
				try {
					forwarderPoint = new InetPoint(InetAddress.getByName(this.bundleForwarderHost).getAddress(),
							this.bundleForwarderPort);
				} catch (UnknownHostException e) {
					logger.error("Error creating forwarder InetPoint ", e);
				}
			}
			
			logger.info(String.format("Forwarder host '%s' and port '%s'.", this.bundleForwarderHost, this.bundleForwarderPort));
			logger.info(String.format("Packet sender forwarding all response: %s", this.forwardAll));
			
			logger.info("Constructing receipt sender ...");
			String brokerUrl = null;
			if (! StringUtils.isEmpty(this.receiptJmsHost) && this.receiptJmsPort != -1) {
				StringBuilder sb = new StringBuilder();
				sb.append("nio://").append(this.receiptJmsHost).append(':').append(this.receiptJmsPort);
				brokerUrl = sb.toString();
			} else {
				brokerUrl = PropertyLocator.getString("messaging.external.connection.url");
			}
			
			if (brokerUrl == null) {
				throw new InitializationException("Missing property 'messaging.external.connection.url'.");
			}
		
			String username = PropertyLocator.getString("messaging.external.connection.user");
			if (username == null) {
				throw new InitializationException("Missing property 'messaging.external.connection.user'.");
			}
		
			String password = PropertyLocator.getString("messaging.external.connection.password");
			if (password == null) {
				throw new InitializationException("Missing property 'messaging.external.connection.password'.");
			}
		
			ReceiptSender.Builder receiptSenderBuilder = new ReceiptSender.Builder();
			receiptSenderBuilder.setBrokerUrl(brokerUrl).setUsername(username)
				.setPassword(password).setTopicName(this.topicName);
			this.receiptSender = receiptSenderBuilder.build();
			
			logger.info("Constructing result processors ...");
			InetPacketSender sender = new InetPacketSender(forwarderPoint);
			sender.setForwardAll(this.forwardAll);
			
			this.advSitDataResultProcessor = RsuAdvSitDataResultProcessor.newInstance(
				this.coder, sender);
			this.advSitDataResultProcessor.setBroadcastInstructionsFieldName(this.broadcastInstructionsFieldName);
			
			this.intersectionSitDataResultProcessor = IntersectionSitDataResultProcessor.newInstance(
					this.coder, sender);
		} catch (Exception ex) {
			throw new InitializationException("Failed to initialize QueryProcessor.", ex);
		}
	}

	public void dispose() {
		if (this.receiptSender != null) {
			this.receiptSender.close();
			this.receiptSender = null;
		}
	}

	@Override
	protected void processInternal(JSONObject record, FlushCounter counter) {
		DataModel model = new DataModel(record);
		try {
			logger.debug("Processing query request: " + record.toString());
			
			Collection<DBObject> result = performQuery(model);
			if (model.isAdvSitDataDistRSE()) {
				this.advSitDataResultProcessor.packageAndSend(model, result);
			} else {
				this.intersectionSitDataResultProcessor.packageAndSend(model, result);
			}
			sendReceipt(model);
			Syslogger.getInstance().log(SYS_LOG_ID, 
					String.format("Processed query request %s", model.toString()));
		} catch (Exception ex) {
			logger.error(String.format("Failed to process query request: %s", record.toString()), ex);
			Syslogger.getInstance().log(SYS_LOG_ID, 
					String.format("Failed to process query request %s", model.toString()));
		} finally {
			counter.noop();
		}
	}
	
	public void flush() {
		logger.debug(String.format("The method flush() is not used by this class '%s'.", this.getClass().getName()));
	}
	
	private Collection<DBObject> performQuery(DataModel model) throws QueryProcessorException {
		if (! model.isQueryRequest()) {
			throw new QueryProcessorException("Invalid query request.");
		}
		
		if (model.getDestHost() == null || model.getDestPort() == null) {
			throw new QueryProcessorException("Destination host/port is missing, ignoring query request.");
		}
		
		BoundingBox bb = buildBoundingBox(model);
		
		Point nwCorner = buildPoint(bb.getNWLat(), bb.getNWLon());
		Point neCorner = buildPoint(bb.getNWLat(), bb.getSELon());
		Point seCorner = buildPoint(bb.getSELat(), bb.getSELon());
		Point swCorner = buildPoint(bb.getSELat(), bb.getNWLon());

		Geometry geometry = buildGeometry(
			Geometry.POLYGON_TYPE, 
			buildCoordinates(
				nwCorner, 
				neCorner, 
				seCorner, 
				swCorner));
		
		// Calculate the oldest advisory / intersection sitdata we want from
		// querying the SDW database. If the time bound is not supplied
		// than we want N latest, N being the limit of the query.
		
		DateRange.Builder builder = null;
		
		Integer timeBound = model.getTimeBound();
		if (timeBound != null) {
			builder = new DateRange.Builder();
			builder.setFieldName(CREATED_AT_FIELD);
			
			int maxAgeInMins = 1000 * 60 * timeBound;
			builder.setStartTime(System.currentTimeMillis() - maxAgeInMins);
		}
		
		DateRange dateRange = (builder != null) ? builder.build() : null;
		
		// When querying the advisory sitdata we order by the created at field
		// while the intersection sitdata we order by the timestamp field. This is
		// the case because advisory sitdata doesn't contain one while the 
		// intersection sitdata contains one in the SPAT message.
		
		if (model.isAdvSitDataDistRSE()) {
			return this.advSitDataDao.findAll(
				this.dbAdvSitDataCollection, 
				this.broadcastInstructionsFieldName, 
				this.geospatialFieldName,
				dateRange,
				CREATED_AT_FIELD,
				geometry,
				RsuAdvSitDataResultProcessor.MAX_RECORDS);
		} else {
			return this.intersectionSitDataDao.findAll(
				this.dbIntersectionSitDataCollection,
				this.geospatialFieldName,
				dateRange,
				CREATED_AT_FIELD,
				geometry,
				IntersectionSitDataResultProcessor.MAX_RECORDS);
		}
	}
	
	private BoundingBox buildBoundingBox(DataModel model) throws QueryProcessorException {
		if (! model.hasNWPosObj() && ! model.hasSEPosObj()) {
			throw new QueryProcessorException("Missing northwest and southeast json object.");
		}
		
		if (model.isBoundingBoxEmpty()) {
			throw new QueryProcessorException("Bounding box is empty.");
		}
		
		if (model.hasNWPosObj() && ! model.hasSEPosObj()) {
			throw new QueryProcessorException("Missing southeast position attribute in record.");
		}
		
		if (! model.hasNWPosObj() && model.hasSEPosObj()) {
			throw new QueryProcessorException("Missing northwest position attribute in record.");
		}
		
		Double nwLat = model.getNWLat();
		Double nwLon = model.getNWLon();
		Double seLat = model.getSELat();
		Double seLon = model.getSELon();
			
		if (nwLat == null) {
			throw new QueryProcessorException("Missing northwest latitude attribute in record.");
		}
			
		if (nwLon == null) {
			throw new QueryProcessorException("Missing northwest longitude attribute in record.");
		}
			
		if (seLat == null) {
			throw new QueryProcessorException("Missing southeast latitude attribute in record.");
		}
			
		if (seLon == null) {
			throw new QueryProcessorException("Missing southeast longitude attribute in record.");
		}
			
		BoundingBox.Builder builder = new BoundingBox.Builder();
		builder.setNWLat(nwLat).setNWLon(nwLon).setSELat(seLat).setSELon(seLon);
		return BoundingBoxValidator.validate(builder.build());
	}
	
	private Point buildPoint(double lat, double lon) {
		Point.Builder builder = new Point.Builder();
		builder.setLat(lat).setLon(lon);
		return builder.build();
	}
	
	private Coordinates buildCoordinates(
			Point nwCorner, 
			Point neCorner, 
			Point seCorner, 
			Point swCorner) {
		Coordinates.Builder builder = new Coordinates.Builder();
		// Note: MongoDB requires that all geometry shape start and end at the same point
		builder.addPoint(nwCorner).addPoint(neCorner).addPoint(seCorner).addPoint(swCorner).addPoint(nwCorner);
		return builder.build();
	}
	
	private Geometry buildGeometry(String type, Coordinates coordinates) {
		Geometry.Builder builder = new Geometry.Builder();
		builder.setType(type).setCoordinates(coordinates);
		return builder.build();
	}
	
	private void sendReceipt(DataModel model) {
		try {
			String receiptId = model.getReceiptId();
			if (receiptId != null) {
				Receipt.Builder builder = new Receipt.Builder();
				builder.setReceiptId(receiptId);
				
				logger.info(String.format("Sent receipt '%s' for '%s'.", model.getReceiptId(), model.getDestHost()));
				this.receiptSender.send(builder.build().toString());
			} else {
				logger.warn("Receipt not sent because 'receiptId' field doesn't exist. Record: " + model.toString());
			}
		} catch (Exception ex) {
			logger.error("Failed to send receipt to external jms server.", ex);
		}
	}
	
}