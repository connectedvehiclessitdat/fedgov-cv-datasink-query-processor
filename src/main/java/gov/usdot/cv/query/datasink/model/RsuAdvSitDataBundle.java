package gov.usdot.cv.query.datasink.model;

import gov.usdot.asn1.generated.j2735.dsrc.DDay;
import gov.usdot.asn1.generated.j2735.dsrc.DFullTime;
import gov.usdot.asn1.generated.j2735.dsrc.DHour;
import gov.usdot.asn1.generated.j2735.dsrc.DMinute;
import gov.usdot.asn1.generated.j2735.dsrc.DMonth;
import gov.usdot.asn1.generated.j2735.dsrc.DYear;
import gov.usdot.asn1.generated.j2735.dsrc.Priority;
import gov.usdot.asn1.generated.j2735.semi.AdvisoryBroadcastType;
import gov.usdot.asn1.generated.j2735.semi.AdvisorySituationData;
import gov.usdot.asn1.generated.j2735.semi.BroadcastInstructions;
import gov.usdot.asn1.generated.j2735.semi.DsrcInstructions;
import gov.usdot.asn1.generated.j2735.semi.Psid;
import gov.usdot.asn1.generated.j2735.semi.AdvisoryBroadcast;
import gov.usdot.asn1.generated.j2735.semi.TxChannel;
import gov.usdot.asn1.generated.j2735.semi.TxMode;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import com.mongodb.BasicDBObject;
import com.oss.asn1.OctetString;

public class RsuAdvSitDataBundle {
	private static final String DATE_PATTERN 	= "yyyy-MM-dd'T'HH:mm:ss";
	private static final String UTC_TIMEZONE	= "UTC";
	
	private static final String ASDM_TYPE_KEY 		= "type";
	private static final String PSID_KEY 			= "psid";
	private static final String PRIORITY_KEY 		= "priority";
	private static final String TX_MODE_KEY 		= "txMode";
	private static final String TX_CHANNEL_KEY 		= "txChannel";
	private static final String TX_INTERVAL_KEY 	= "txInterval";
	private static final String DELIVERY_START_KEY 	= "deliveryStart";
	private static final String DELIVERY_STOP_KEY 	= "deliveryStop";
	private static final String SIGNATURE_KEY 		= "signature";
	private static final String ENCRYPTION_KEY 		= "encryption";
	
	private BasicDBObject broadcastDataModel;
	private AdvisorySituationData advSitData;
	private AdvisoryBroadcast cache;
	
	private RsuAdvSitDataBundle(
			BasicDBObject broadcastDataModel, 
			AdvisorySituationData advSitData) {
		this.broadcastDataModel = broadcastDataModel;
		this.advSitData = advSitData;
	}
	
	public AdvisoryBroadcast getRSUAdvisoryBroadcast() throws ParseException {
		if (this.cache != null) return this.cache;
		
		if (! this.broadcastDataModel.containsField(ASDM_TYPE_KEY)) return null;
		int asdmType = this.broadcastDataModel.getInt(ASDM_TYPE_KEY);
		
		if (! this.broadcastDataModel.containsField(PSID_KEY)) return null;
		int psid = this.broadcastDataModel.getInt(PSID_KEY);
		
		if (! this.broadcastDataModel.containsField(PRIORITY_KEY)) return null;
		int priority = this.broadcastDataModel.getInt(PRIORITY_KEY);
		
		if (! this.broadcastDataModel.containsField(TX_MODE_KEY)) return null;
		long txMode = this.broadcastDataModel.getLong(TX_MODE_KEY);
		
		if (! this.broadcastDataModel.containsField(TX_CHANNEL_KEY)) return null;
		long txChannel = this.broadcastDataModel.getLong(TX_CHANNEL_KEY);
		
		if (! this.broadcastDataModel.containsField(TX_INTERVAL_KEY)) return null;
		long txInterval = this.broadcastDataModel.getLong(TX_INTERVAL_KEY);
		
		DateFormat formatter = new SimpleDateFormat(DATE_PATTERN);
		
		if (! this.broadcastDataModel.containsField(DELIVERY_START_KEY)) return null;
		Calendar start = Calendar.getInstance(TimeZone.getTimeZone(UTC_TIMEZONE));
		start.setTimeInMillis(formatter.parse(this.broadcastDataModel.getString(DELIVERY_START_KEY)).getTime());
		
		if (! this.broadcastDataModel.containsField(DELIVERY_STOP_KEY)) return null;
		Calendar stop = Calendar.getInstance(TimeZone.getTimeZone(UTC_TIMEZONE));
		stop.setTimeInMillis(formatter.parse(this.broadcastDataModel.getString(DELIVERY_STOP_KEY)).getTime());
		
		if (! this.broadcastDataModel.containsField(SIGNATURE_KEY)) return null;
		boolean signature = this.broadcastDataModel.getBoolean(SIGNATURE_KEY, true);
		
		if (! this.broadcastDataModel.containsField(ENCRYPTION_KEY)) return null;
		boolean encryption = this.broadcastDataModel.getBoolean(ENCRYPTION_KEY, false);
		
		this.cache = buildRSUAdvisoryBroadcast(
			asdmType, 
			psid, 
			priority, 
			txMode,
			txChannel, 
			txInterval, 
			start, 
			stop, 
			signature, 
			encryption);
		
		return this.cache;
	}
	
	private AdvisoryBroadcast buildRSUAdvisoryBroadcast(
			int asdmType,
			int psid,
			int priority,
			long txMode,
			long txChannel,
			long txInterval,
			Calendar start,
			Calendar stop,
			boolean signature,
			boolean encryption) {
		AdvisoryBroadcast rsuAdvBroadcast = new AdvisoryBroadcast();
		rsuAdvBroadcast.setMessagePsid(new Psid(ByteBuffer.allocate(4).putInt(psid).array()));
		
		OctetString advisoryMessage = advSitData.getAsdmDetails().getAdvisoryMessage();
		rsuAdvBroadcast.setAdvisoryMessage(advisoryMessage);
		
		BroadcastInstructions  broadcastInst = new BroadcastInstructions();
		broadcastInst.setBiType(AdvisoryBroadcastType.valueOf(asdmType));
		broadcastInst.setBiPriority(new Priority(Integer.toString(priority).getBytes()));

		DFullTime biDeliveryStart = new DFullTime();
		biDeliveryStart.setYear(new DYear(start.get(Calendar.YEAR)));
		biDeliveryStart.setMonth(new DMonth(start.get(Calendar.MONTH)));
		biDeliveryStart.setDay(new DDay(start.get(Calendar.DAY_OF_MONTH)));
		biDeliveryStart.setHour(new DHour(start.get(Calendar.HOUR_OF_DAY)));
		biDeliveryStart.setMinute(new DMinute(start.get(Calendar.MINUTE)));
		broadcastInst.setBiDeliveryStart(biDeliveryStart);
		
		DFullTime biDelievryStop = new DFullTime();
		biDelievryStop.setYear(new DYear(stop.get(Calendar.YEAR)));
		biDelievryStop.setMonth(new DMonth(stop.get(Calendar.MONTH)));
		biDelievryStop.setDay(new DDay(stop.get(Calendar.DAY_OF_MONTH)));
		biDelievryStop.setHour(new DHour(stop.get(Calendar.HOUR_OF_DAY)));
		biDelievryStop.setMinute(new DMinute(stop.get(Calendar.MINUTE)));
		broadcastInst.setBiDeliveryStop(biDelievryStop);
		
		broadcastInst.setBiSignature(signature);
		broadcastInst.setBiEncryption(encryption);
		
		DsrcInstructions dsrcInst = new DsrcInstructions();
		dsrcInst.setBiTxMode(TxMode.valueOf(txMode));
		dsrcInst.setBiTxChannel(TxChannel.valueOf(txChannel));
		dsrcInst.setBiTxInterval(txInterval);
		broadcastInst.setDsrcInst(dsrcInst);
		
		rsuAdvBroadcast.setBroadcastInst(broadcastInst);
		
		return rsuAdvBroadcast;
	}
	
	public static class Builder {
		private BasicDBObject broadcastDataModel;
		private AdvisorySituationData advSitData;
		
		public Builder setBroadcastDataModel(BasicDBObject broadcastDataModel) {
			this.broadcastDataModel = broadcastDataModel;
			return this;
		}
		
		public Builder setAdvSitData(AdvisorySituationData advSitData) {
			this.advSitData = advSitData;
			return this;
		}
		
		public RsuAdvSitDataBundle build() {
			if (this.broadcastDataModel != null && this.advSitData != null) {
				return new RsuAdvSitDataBundle(this.broadcastDataModel, this.advSitData);
			}
			return null;
		}
	}
}