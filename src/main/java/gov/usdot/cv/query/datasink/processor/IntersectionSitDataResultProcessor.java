package gov.usdot.cv.query.datasink.processor;

import gov.usdot.asn1.generated.j2735.dsrc.TemporaryID;
import gov.usdot.asn1.generated.j2735.semi.IntersectionBundle;
import gov.usdot.asn1.generated.j2735.semi.IntersectionBundle.IsdRecords;
import gov.usdot.asn1.generated.j2735.semi.IntersectionRecord;
import gov.usdot.asn1.generated.j2735.semi.IntersectionSituationData;
import gov.usdot.asn1.generated.j2735.semi.IntersectionSituationDataBundle;
import gov.usdot.asn1.generated.j2735.semi.IntersectionSituationDataBundle.IsdBundles;
import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;
import gov.usdot.asn1.generated.j2735.semi.SemiSequenceID;
import gov.usdot.asn1.j2735.J2735Util;
import gov.usdot.cv.common.asn1.GroupIDHelper;
import gov.usdot.cv.common.asn1.TemporaryIDHelper;
import gov.usdot.cv.common.inet.InetPacketSender;
import gov.usdot.cv.common.inet.InetPoint;
import gov.usdot.cv.query.datasink.model.DataModel;
import gov.usdot.cv.query.datasink.model.IntersectionSitDataBundle;
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

public class IntersectionSitDataResultProcessor extends AbstractSitDataResultProcessor {

	private static final Logger logger = Logger.getLogger(IntersectionSitDataResultProcessor.class);
	
	/** Max number of IntersectionBundle */
	public static final int MAX_BUNDLES 								= 60;
	
	/** Number of IntersectionBundle perIntersectionSituationDataBundle */
	public static final int MAX_BUNDLE_PER_INTERSECTION_SITDATA_BUNDLE 	= 5;
	
	/** Number of IntersectionRecord per IntersectionBundle */
	public static final int MAX_RECORDS_PER_BUNDLE 						= 5;
	
	/** Max number of IntersectionRecord */
	public static final int MAX_RECORDS 								= MAX_BUNDLES * MAX_RECORDS_PER_BUNDLE;
	
	private class IsdBundlePackage {
		int bundleId = -1;
		List<IntersectionSituationDataBundle> bundles = null;;
	}
	
	private CryptoProvider cryptoProvider = new CryptoProvider();
	
	public static IntersectionSitDataResultProcessor newInstance(
		Coder coder, 
		InetPacketSender sender) {
		return new IntersectionSitDataResultProcessor(coder, sender);
	}
	
	private IntersectionSitDataResultProcessor(
			Coder coder,
			InetPacketSender sender) {
		super(coder, sender);
		SecurityHelper.initSecurity();
	}

	@Override
	public void packageAndSend(DataModel model, Collection<DBObject> result) throws Exception {
		List<IntersectionRecord> records = extractIsdRecords(result);
		IsdBundlePackage pkg = buildIsdBundlePackege(model, records);
		sendIsdBundlePackage(model, pkg);
	}
	
	private List<IntersectionRecord> extractIsdRecords(Collection<DBObject> result) throws DecodeFailedException, DecodeNotSupportedException {
		List<IntersectionRecord> records = new ArrayList<IntersectionRecord>();
		
		Iterator<DBObject> it = result.iterator();
		while(it.hasNext()) {
			DBObject dbObj = it.next();
			if (dbObj instanceof BasicDBObject) {
				IntersectionSitDataBundle.Builder builder = new IntersectionSitDataBundle.Builder();
				
				BasicDBObject intersectionSitDataObj = (BasicDBObject) dbObj;
				if (intersectionSitDataObj.containsField(ENCODED_MSG_KEY)) {
					byte [] message = Base64.decodeBase64(intersectionSitDataObj.getString(ENCODED_MSG_KEY));
					AbstractData berEncoded = J2735Util.decode(getCoder(), message);
					if (berEncoded instanceof IntersectionSituationData) {
						builder.setIntersectionSitData((IntersectionSituationData) berEncoded);
					} else {
						logger.debug(String.format("Encoded message is not of type IntersectionSituationData: %s", berEncoded.toString()));
						continue;
					}
				}
				
				IntersectionSitDataBundle intersectionSitDataBundle = builder.build();
				if (intersectionSitDataBundle != null) {
					try {
						records.add(intersectionSitDataBundle.getIntersectionRecord());
					} catch (Exception ex) {
						logger.error("Failed to build intersection record object.", ex);
					}
				}
			}
		}
		
		return records;
	}
	
	private IsdBundlePackage buildIsdBundlePackege(
			DataModel model, 
			List<IntersectionRecord> records) {
		if (records == null || records.size() == 0) {
			logger.debug("0 IntersectionRecord records where found from the database.");
			
			IntersectionSituationDataBundle bundle = new IntersectionSituationDataBundle();
			bundle.setDialogID(SemiDialogID.intersectionSitDataQuery);
			bundle.setSeqID(SemiSequenceID.data);
			bundle.setRequestID(TemporaryIDHelper.toTemporaryID(model.getRequestId()));
			bundle.setGroupID(GroupIDHelper.toGroupID(model.getGroupId()));
			bundle.setRecordCount(new INTEGER(0));
			bundle.setBundleCount(new INTEGER(0));
			bundle.setIsdBundles(new IsdBundles());
			
			IsdBundlePackage pkg = new IsdBundlePackage();
			pkg.bundles = Collections.singletonList(bundle);
			return pkg;
		}
		
		logger.debug(records.size() + " IntersectionRecord records where found from the database.");
		
		int tmpBundleId = nextBundleId();
		TemporaryID bundleId = TemporaryIDHelper.toTemporaryID(tmpBundleId);
		
		int recordCount = records.size();
		int bundleCount = recordCount / MAX_RECORDS_PER_BUNDLE;
		if (recordCount % MAX_RECORDS_PER_BUNDLE != 0) bundleCount++;
		
		int bundleNumber = 0;
		List<IntersectionBundle> bundles = new ArrayList<IntersectionBundle>();
		
		IsdRecords isdRecords = new IsdRecords();
		for (int i = 0; i < recordCount; i++) {
			if (isdRecords.getSize() == MAX_RECORDS_PER_BUNDLE) {
				bundleNumber++;
				IntersectionBundle bundle = new IntersectionBundle();
				if (bundleNumber % MAX_BUNDLE_PER_INTERSECTION_SITDATA_BUNDLE == 0) {
					bundle.setBundleNumber(MAX_BUNDLE_PER_INTERSECTION_SITDATA_BUNDLE);
				} else {
					bundle.setBundleNumber(bundleNumber % MAX_BUNDLE_PER_INTERSECTION_SITDATA_BUNDLE);
				}
				bundle.setBundleId(bundleId);
				bundle.setIsdRecords(isdRecords);
				bundles.add(bundle);
				
				isdRecords = new IsdRecords();
			}
			isdRecords.add(records.get(i));
		}
		
		if (isdRecords.getSize() > 0) {
			bundleNumber++;
			IntersectionBundle bundle = new IntersectionBundle();
			if (bundleNumber % MAX_BUNDLE_PER_INTERSECTION_SITDATA_BUNDLE == 0) {
				bundle.setBundleNumber(MAX_BUNDLE_PER_INTERSECTION_SITDATA_BUNDLE);
			} else {
				bundle.setBundleNumber(bundleNumber % MAX_BUNDLE_PER_INTERSECTION_SITDATA_BUNDLE);
			}
			bundle.setBundleId(bundleId);
			bundle.setIsdRecords(isdRecords);
			bundles.add(bundle);
		}
		
		logger.debug("Packaged " + bundles.size() + " isd record bundle(s).");
		
		return buildIsdBundlePackage(
			tmpBundleId,
			recordCount, 
			bundleCount, 
			model.getRequestId(), 
			model.getGroupId(),
			bundles);
	}
	
	private IsdBundlePackage buildIsdBundlePackage(
		int bundleId,
		int recordCount, 
		int bundleCount,
		int requestId,
		int groupId,
		List<IntersectionBundle> intersectionBundles) {
		List<IntersectionSituationDataBundle> bundles = new ArrayList<IntersectionSituationDataBundle>();
		
		IsdBundles isdBundles = new IsdBundles();
		for (int i = 0; i < intersectionBundles.size(); i++) {
			if (isdBundles.getSize() == MAX_BUNDLE_PER_INTERSECTION_SITDATA_BUNDLE) {
				logger.debug("Constructing intersection situation data bundle with " + isdBundles.getSize() + " isd bundle(s) ...");
				IntersectionSituationDataBundle isdBundle = new IntersectionSituationDataBundle();
				isdBundle.setDialogID(SemiDialogID.intersectionSitDataQuery);
				isdBundle.setSeqID(SemiSequenceID.data);
				isdBundle.setRequestID(TemporaryIDHelper.toTemporaryID(requestId));
				isdBundle.setGroupID(GroupIDHelper.toGroupID(groupId));
				isdBundle.setRecordCount(new INTEGER(recordCount));
				isdBundle.setBundleCount(new INTEGER(bundleCount));
				isdBundle.setIsdBundles(isdBundles);
				bundles.add(isdBundle);
				
				isdBundles = new IsdBundles();
			} 
			isdBundles.add(intersectionBundles.get(i));
		}
		
		if (isdBundles.getSize() > 0) {
			logger.debug("Constructing intersection situation data bundle with " + isdBundles.getSize() + " isd bundle(s) ...");
			IntersectionSituationDataBundle isdBundle = new IntersectionSituationDataBundle();
			isdBundle.setDialogID(SemiDialogID.intersectionSitDataQuery);
			isdBundle.setSeqID(SemiSequenceID.data);
			isdBundle.setRequestID(TemporaryIDHelper.toTemporaryID(requestId));
			isdBundle.setGroupID(GroupIDHelper.toGroupID(groupId));
			isdBundle.setRecordCount(new INTEGER(recordCount));
			isdBundle.setBundleCount(new INTEGER(bundleCount));
			isdBundle.setIsdBundles(isdBundles);
			bundles.add(isdBundle);
		}
		
		logger.debug("Packaged " + bundles.size() + " intersection situation data bundle(s).");
		
		IsdBundlePackage pkg = new IsdBundlePackage();
		pkg.bundleId = bundleId;
		pkg.bundles = bundles;
		return pkg;
	}
	
	private void sendIsdBundlePackage(
			DataModel model,
			IsdBundlePackage pkg) throws Exception {
		for (IntersectionSituationDataBundle bundle : pkg.bundles) {
			int isdCount = (bundle.getIsdBundles() != null) ? bundle.getIsdBundles().getSize() : 0;
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
					logger.debug(String.format("Sending intersection situation data bundle (id=%s,rcount=%s,bcount=%s,isdcount=%s)", 
							pkg.bundleId, bundle.getRecordCount(), bundle.getBundleCount(), isdCount));
					InetPoint destPoint = new InetPoint(model.getDestHost(), model.getDestPort(), getForwardAll());
					getDataBundleSender().forward(destPoint, payload, model.fromForwarder());
					sent = true;
					break;
				} catch (Exception ex) {
					logger.error(String.format("Failed to send intersection situation data bundle (id=%s,rcount=%s,bcount=%s,isdcount=%s) to %s on port %s.", 
						pkg.bundleId, bundle.getRecordCount(), bundle.getBundleCount(), isdCount, model.getDestHost(), model.getDestPort()), ex);
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