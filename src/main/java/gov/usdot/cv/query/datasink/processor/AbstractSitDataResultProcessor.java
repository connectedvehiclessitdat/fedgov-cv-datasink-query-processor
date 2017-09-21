package gov.usdot.cv.query.datasink.processor;

import gov.usdot.cv.common.inet.InetPacketSender;
import gov.usdot.cv.query.datasink.model.DataModel;

import java.util.Collection;
import java.util.Random;

import com.mongodb.DBObject;
import com.oss.asn1.Coder;

public abstract class AbstractSitDataResultProcessor {
	
	protected static final String ENCODED_MSG_KEY = "encodedMsg";
	
	private Coder coder;
	private InetPacketSender dataBundleSender;
	private Random bundleIdGenerator = new Random();
	
	protected AbstractSitDataResultProcessor(
		Coder coder, 
		InetPacketSender dataBundleSender) {
		this.coder = coder;
		this.dataBundleSender = dataBundleSender;
	}
	
	abstract void packageAndSend(DataModel model, Collection<DBObject> result) throws Exception;
	
	protected Coder getCoder() { return this.coder; }
	protected InetPacketSender getDataBundleSender() { return this.dataBundleSender; }
	protected boolean getForwardAll() { return dataBundleSender.isForwardAll(); }
	protected int nextBundleId() { return this.bundleIdGenerator.nextInt(); }
	
}