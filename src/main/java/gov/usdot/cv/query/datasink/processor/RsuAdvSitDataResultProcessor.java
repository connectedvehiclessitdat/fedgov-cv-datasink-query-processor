package gov.usdot.cv.query.datasink.processor;

import gov.usdot.asn1.generated.j2735.dsrc.TemporaryID;
import gov.usdot.asn1.generated.j2735.semi.AdvisoryBroadcast;
import gov.usdot.asn1.generated.j2735.semi.AdvisorySituationBundle;
import gov.usdot.asn1.generated.j2735.semi.AdvisorySituationBundle.AsdRecords;
import gov.usdot.asn1.generated.j2735.semi.AdvisorySituationData;
import gov.usdot.asn1.generated.j2735.semi.AdvisorySituationDataDistribution;
import gov.usdot.asn1.generated.j2735.semi.AdvisorySituationDataDistribution.AsdBundles;
import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;
import gov.usdot.asn1.generated.j2735.semi.SemiSequenceID;
import gov.usdot.asn1.j2735.J2735Util;
import gov.usdot.cv.common.asn1.GroupIDHelper;
import gov.usdot.cv.common.asn1.TemporaryIDHelper;
import gov.usdot.cv.common.inet.InetPacketSender;
import gov.usdot.cv.common.inet.InetPoint;
import gov.usdot.cv.query.datasink.model.DataModel;
import gov.usdot.cv.query.datasink.model.RsuAdvSitDataBundle;
import gov.usdot.cv.security.SecurityHelper;
import gov.usdot.cv.security.crypto.CryptoProvider;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.oss.asn1.AbstractData;
import com.oss.asn1.Coder;
import com.oss.asn1.DecodeFailedException;
import com.oss.asn1.DecodeNotSupportedException;
import com.oss.asn1.INTEGER;

public class RsuAdvSitDataResultProcessor extends AbstractSitDataResultProcessor {
	
	private static final Logger logger = Logger.getLogger(RsuAdvSitDataResultProcessor.class);
	
	/** Max number of AdvisorySituationBundle */
	public static final int MAX_BUNDLES 					= 40;
	
	/** Number of AdvisorySituationBundle per AdvisorySituationDataDistribution */
	public static final int MAX_BUNDLE_PER_RSU_BUNDLE 		= 4;
	
	/** Number of AdvisoryBroadcast per AdvisorySituationBundle */
	public static final int MAX_RECORDS_PER_BUNDLE 			= 10; 
	
	/** Max number of AdvisoryBroadcast */
	public static final int MAX_RECORDS 					= MAX_BUNDLES * MAX_RECORDS_PER_BUNDLE;
	
	private String broadcastInstructionsFieldName;
	
	private CryptoProvider cryptoProvider = new CryptoProvider();
	
	private class RsuAsdBundlePackage {
		int bundleId = -1;
		List<AdvisorySituationDataDistribution> bundles = null;;
	}
	
	public static RsuAdvSitDataResultProcessor newInstance(
		Coder coder, 
		InetPacketSender sender) {
		return new RsuAdvSitDataResultProcessor(coder, sender);
	}

	private RsuAdvSitDataResultProcessor(
		Coder coder, 
		InetPacketSender sender) { 
		super(coder, sender);
	}
	
	public void setBroadcastInstructionsFieldName(String broadcastInstructionsFieldName) {
		this.broadcastInstructionsFieldName = broadcastInstructionsFieldName;
	}
	
	@Override
	public void packageAndSend(DataModel model, Collection<DBObject> result) throws Exception {
		List<AdvisoryBroadcast> records = extractAsdRecords(result);
		RsuAsdBundlePackage pkg = buildRsuAsdBundlePackage(model, records);
		sendRsuAsdBundlePackage(model, pkg);
	}
	
	private List<AdvisoryBroadcast> extractAsdRecords(Collection<DBObject> result) throws DecodeFailedException, DecodeNotSupportedException {
		List<AdvisoryBroadcast> records = new ArrayList<AdvisoryBroadcast>();
		
		Iterator<DBObject> it = result.iterator();
		while(it.hasNext()) {
			DBObject dbObj = it.next();
			if (dbObj instanceof BasicDBObject) {
				RsuAdvSitDataBundle.Builder builder = new RsuAdvSitDataBundle.Builder();
				
				BasicDBObject advSitDataObj = (BasicDBObject) dbObj;
				if (advSitDataObj.containsField(ENCODED_MSG_KEY)) {
					byte [] message = Base64.decodeBase64(advSitDataObj.getString(ENCODED_MSG_KEY));
					//logger.debug(String.format("Decoding AdvisorySituationData message: %s", Hex.encodeHexString(message)));
					AbstractData berEncoded = J2735Util.decode(getCoder(), message);
					if (berEncoded instanceof AdvisorySituationData) {
						builder.setAdvSitData((AdvisorySituationData) berEncoded);
					} else {
						logger.debug(String.format("Encoded message is not of type AdvisorySituationData: %s", berEncoded.toString()));
						continue;
					}
				}
				
				if (advSitDataObj.containsField(this.broadcastInstructionsFieldName)) {
					builder.setBroadcastDataModel((BasicDBObject) advSitDataObj.get(this.broadcastInstructionsFieldName));
				}
				
				RsuAdvSitDataBundle advSitDataBundle = builder.build();
				if (advSitDataBundle != null) {
					try {
						records.add(advSitDataBundle.getRSUAdvisoryBroadcast());
					} catch (Exception ex) {
						logger.error("Failed to build rsu advisory broadcast object.", ex);
					}
				}
			}
		}
		
		return records;
	}
	
	private RsuAsdBundlePackage buildRsuAsdBundlePackage(
			DataModel model, 
			List<AdvisoryBroadcast> records) {
		if (records == null || records.size() == 0) {
			logger.debug("0 AdvisoryBroadcast records where found from the database.");
			
			AdvisorySituationDataDistribution bundle = new AdvisorySituationDataDistribution();
			bundle.setDialogID(SemiDialogID.advSitDatDist);
			bundle.setSeqID(SemiSequenceID.data);
			bundle.setRequestID(TemporaryIDHelper.toTemporaryID(model.getRequestId()));
			bundle.setGroupID(GroupIDHelper.toGroupID(model.getGroupId()));
			bundle.setRecordCount(new INTEGER(0));
			bundle.setBundleCount(new INTEGER(0));
			bundle.setAsdBundles(new AsdBundles());
			
			RsuAsdBundlePackage pkg = new RsuAsdBundlePackage();
			pkg.bundles = Collections.singletonList(bundle);
			return pkg;
		}
		
		logger.debug(records.size() + " AdvisoryBroadcast records where found from the database.");
		
		int tmpBundleId = nextBundleId();
		TemporaryID bundleId = TemporaryIDHelper.toTemporaryID(tmpBundleId);
		
		int recordCount = records.size();
		int bundleCount = recordCount / MAX_RECORDS_PER_BUNDLE;
		if (recordCount % MAX_RECORDS_PER_BUNDLE != 0) bundleCount++;
		
		int bundleNumber = 0;
		List<AdvisorySituationBundle> bundles = new ArrayList<AdvisorySituationBundle>();
		
		AsdRecords asdRecords = new AsdRecords();
		for (int i = 0; i < recordCount; i++) {
			if (asdRecords.getSize() == MAX_RECORDS_PER_BUNDLE) {
				bundleNumber++;
				AdvisorySituationBundle bundle = new AdvisorySituationBundle();
				if (bundleNumber % MAX_BUNDLE_PER_RSU_BUNDLE == 0) {
					bundle.setBundleNumber(MAX_BUNDLE_PER_RSU_BUNDLE);
				} else {
					bundle.setBundleNumber(bundleNumber % MAX_BUNDLE_PER_RSU_BUNDLE);
				}
				bundle.setBundleId(bundleId);
				bundle.setAsdRecords(asdRecords);
				bundles.add(bundle);
				
				asdRecords = new AsdRecords();
			} 
			asdRecords.add(records.get(i));
		}
		
		if (asdRecords.getSize() > 0) {
			bundleNumber++;
			AdvisorySituationBundle bundle = new AdvisorySituationBundle();
			if (bundleNumber % MAX_BUNDLE_PER_RSU_BUNDLE == 0) {
				bundle.setBundleNumber(MAX_BUNDLE_PER_RSU_BUNDLE);
			} else {
				bundle.setBundleNumber(bundleNumber % MAX_BUNDLE_PER_RSU_BUNDLE);
			}
			bundle.setBundleId(bundleId);
			bundle.setAsdRecords(asdRecords);
			bundles.add(bundle);
		}
		
		logger.debug("Packaged " + bundles.size() + " asd record bundle(s).");
		
		return buildRsuAsdBundlePackage(
			tmpBundleId,
			recordCount, 
			bundleCount, 
			model.getRequestId(), 
			model.getGroupId(),
			bundles);
	}
	
	private RsuAsdBundlePackage buildRsuAsdBundlePackage(
		int bundleId,
		int recordCount, 
		int bundleCount,
		int requestId,
		int groupId,
		List<AdvisorySituationBundle> advSitDataBundles) {
		List<AdvisorySituationDataDistribution> bundles = new ArrayList<AdvisorySituationDataDistribution>();
		
		AsdBundles asdBundles = new AsdBundles();
		for (int i = 0; i < advSitDataBundles.size(); i++) {
			if (asdBundles.getSize() == MAX_BUNDLE_PER_RSU_BUNDLE) {
				logger.debug("Constructing rsu advisory situation data bundle with " + asdBundles.getSize() + " asd bundle(s) ...");
				AdvisorySituationDataDistribution rsuBundle = new AdvisorySituationDataDistribution();
				rsuBundle.setDialogID(SemiDialogID.advSitDatDist);
				rsuBundle.setSeqID(SemiSequenceID.data);
				rsuBundle.setRequestID(TemporaryIDHelper.toTemporaryID(requestId));
				rsuBundle.setGroupID(GroupIDHelper.toGroupID(groupId));
				rsuBundle.setRecordCount(new INTEGER(recordCount));
				rsuBundle.setBundleCount(new INTEGER(bundleCount));
				rsuBundle.setAsdBundles(asdBundles);
				bundles.add(rsuBundle);
				
				asdBundles = new AsdBundles();
			} 
			asdBundles.add(advSitDataBundles.get(i));
		}
		
		if (asdBundles.getSize() > 0) {
			logger.debug("Constructing rsu advisory situation data bundle with " + asdBundles.getSize() + " asd bundle(s) ...");
			AdvisorySituationDataDistribution rsuBundle = new AdvisorySituationDataDistribution();
			rsuBundle.setDialogID(SemiDialogID.advSitDatDist);
			rsuBundle.setSeqID(SemiSequenceID.data);
			rsuBundle.setRequestID(TemporaryIDHelper.toTemporaryID(requestId));
			rsuBundle.setGroupID(GroupIDHelper.toGroupID(groupId));
			rsuBundle.setRecordCount(new INTEGER(recordCount));
			rsuBundle.setBundleCount(new INTEGER(bundleCount));
			rsuBundle.setAsdBundles(asdBundles);
			bundles.add(rsuBundle);
		}
		
		logger.debug("Packaged " + bundles.size() + " rsu advisory situation data bundle(s).");
		
		RsuAsdBundlePackage pkg = new RsuAsdBundlePackage();
		pkg.bundleId = bundleId;
		pkg.bundles = bundles;
		return pkg;
	}
	
	private void sendRsuAsdBundlePackage(
			DataModel model, 
			RsuAsdBundlePackage pkg) throws Exception {
		for (AdvisorySituationDataDistribution bundle : pkg.bundles) {
			int asdCount = (bundle.getAsdBundles() != null) ? bundle.getAsdBundles().getSize() : 0;
			ByteArrayOutputStream sink = new ByteArrayOutputStream();
			getCoder().encode(bundle, sink);
			byte [] payload = sink.toByteArray();
			
			byte[] certificate = model.getCertificate() != null ? Base64.decodeBase64(model.getCertificate()): null;
			if (certificate != null) {
				try {
					byte[] certID8 = SecurityHelper.registerCert(certificate, cryptoProvider);
					payload = SecurityHelper.encrypt(payload, certID8, cryptoProvider, SecurityHelper.DEFAULT_PSID);
				} catch (Exception ex) {
					logger.error("Couldn't encrypt outgoing message. Reason: " + ex.getMessage(), ex);
				}
			}
			
			int retries = 3;
			Exception lastEx = null;
			boolean sent = false;
			while (retries > 0) {
				try {
					logger.debug(String.format("Sending intersection situation data bundle (id=%s,rcount=%s,bcount=%s,asdcount=%s)", 
							pkg.bundleId, bundle.getRecordCount(), bundle.getBundleCount(), asdCount));
					InetPoint destPoint = new InetPoint(model.getDestHost(), model.getDestPort(), getForwardAll());
					getDataBundleSender().forward(destPoint, payload, model.fromForwarder());
					sent = true;
					break;
				} catch (Exception ex) {
					logger.error(String.format("Failed to send rsu advisory situation data bundle (id=%s,rcount=%s,bcount=%s,asdcount=%s) to %s on port %s.", 
						pkg.bundleId, bundle.getRecordCount(), bundle.getBundleCount(), asdCount, model.getDestHost(), model.getDestPort()), ex);
					lastEx = ex;
				} finally {
					retries--;
				}
				
				try { Thread.sleep(10); } catch (InterruptedException ignore) {}
			}
			
			if (! sent && lastEx != null) throw lastEx;
			
			try { Thread.sleep(10); } catch (InterruptedException ignore) {}
		}
	}
	
}