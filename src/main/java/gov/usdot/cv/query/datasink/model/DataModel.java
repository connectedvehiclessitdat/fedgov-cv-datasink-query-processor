package gov.usdot.cv.query.datasink.model;

import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;
import gov.usdot.asn1.generated.j2735.semi.SemiSequenceID;
import net.sf.json.JSONObject;

/**
 * Expecting the query request data input data model to be of the form.
 * {
 *   receiptId      : <string>
 *   dialogId 		: <number>,
 *   sequenceId 	: <number>,
 *   requestId 		: <number>,
 *   groupId		: <number>,
 *   destHost 		: <string>,
 *   destPort 		: <number>,
 *   fromForwarder 	: <string>,
 *   certificate	: <string>,
 *   timeBound		: <number>,
 *   nwPos: 
 * 	 {
 * 	    lat : <number>,
 *      lon : <number>	
 * 	 },
 * 	 sePos:
 * 	 {
 *      lat : <number>
 *      lon : <number>
 * 	 }
 * }
 */
public class DataModel {
	public static final String RECEIPT_ID_KEY 		= "receiptId";
	public static final String DIALOG_ID_KEY 		= "dialogId";
	public static final String SEQUENCE_ID_KEY		= "sequenceId";
	public static final String REQUEST_ID_KEY		= "requestId";
	public static final String GROUP_ID_KEY		    = "groupId";
	public static final String DEST_HOST_KEY		= "destHost";
	public static final String DEST_PORT_KEY		= "destPort";
	public static final String FROM_FORWARDER_KEY	= "fromForwarder";
	public static final String CERTIFICATE_KEY		= "certificate";
	public static final String TIME_BOUND_KEY		= "timeBound";
	public static final String NW_POS_KEY 			= "nwPos";
	public static final String SE_POS_KEY 			= "sePos";
	public static final String LAT_KEY 				= "lat";
	public static final String LON_KEY 				= "lon";
	
	private JSONObject record;
	
	public DataModel(JSONObject record) {
		this.record = record;
	}
	
	public boolean isQueryRequest() {
		SemiDialogID dialogId = getSemiDialogID();
		SemiSequenceID seqId = getSemiSequenceID();
		return (dialogId != null && 
				seqId != null && 
				(dialogId == SemiDialogID.advSitDatDist || 
				 dialogId == SemiDialogID.intersectionSitDataQuery) && 
				seqId == SemiSequenceID.dataReq);
	}
	
	public boolean isSemiDialogIdValid() {
		SemiDialogID dialogId = getSemiDialogID();
		return (dialogId != null && (dialogId == SemiDialogID.advSitDatDist || 
				dialogId == SemiDialogID.intersectionSitDataQuery));
	}
	
	public boolean isAdvSitDataDistRSE() {
		SemiDialogID dialogId = getSemiDialogID();
		return (dialogId != null && dialogId == SemiDialogID.advSitDatDist);
	}
	
	public boolean isIntersectionSitDataQuery() {
		SemiDialogID dialogId = getSemiDialogID();
		return (dialogId != null && dialogId == SemiDialogID.intersectionSitDataQuery);
	}
	
	public boolean isSemiSequenceIdValid() {
		SemiSequenceID seqId = getSemiSequenceID();
		return (seqId != null && seqId == SemiSequenceID.dataReq);
	}
	
	public String getReceiptId() {
		if (record.has(RECEIPT_ID_KEY)) {
			return record.getString(RECEIPT_ID_KEY);
		}
		return null;
	}
	
	public SemiDialogID getSemiDialogID() {
		if (record.has(DIALOG_ID_KEY)) { 
			return SemiDialogID.valueOf(record.getInt(DIALOG_ID_KEY));
		} 
		return null;
	}
	
	public SemiSequenceID getSemiSequenceID() {
		if (record.has(SEQUENCE_ID_KEY)) { 
			return SemiSequenceID.valueOf(record.getInt(SEQUENCE_ID_KEY));
		} 
		return null;
	}
	
	public Integer getRequestId() {
		if (record.has(REQUEST_ID_KEY)) {
			return record.getInt(REQUEST_ID_KEY);
		}
		return null;
	}
	
	public Integer getGroupId() {
		if (record.has(GROUP_ID_KEY)) {
			return record.getInt(GROUP_ID_KEY);
		}
		return null;
	}
	
	public String getDestHost() {
		if (record.has(DEST_HOST_KEY)) {
			return record.getString(DEST_HOST_KEY);
		}
		return null;
	}
	
	public Integer getDestPort() {
		if (record.has(DEST_PORT_KEY)) {
			return record.getInt(DEST_PORT_KEY);
		}
		return null;
	}
	
	public Boolean fromForwarder() {
		if (record.has(FROM_FORWARDER_KEY)) {
			return Boolean.valueOf(record.getString(FROM_FORWARDER_KEY));
		}
		return null;
	}
	
	public String getCertificate() {
		if (record.has(CERTIFICATE_KEY)) {
			return record.getString(CERTIFICATE_KEY);
		}
		return null;
	}
	
	public Integer getTimeBound() {
		if (record.has(TIME_BOUND_KEY)) {
			return record.getInt(TIME_BOUND_KEY);
		}
		return null;
	}
	
	public boolean hasNWPosObj() {
		return record.has(NW_POS_KEY);
	}
	
	public boolean hasSEPosObj() {
		return record.has(SE_POS_KEY);
	}
	
	public Double getNWLat() {
		JSONObject nwObj = record.getJSONObject(NW_POS_KEY);
		if (nwObj != null && nwObj.has(LAT_KEY)) {
			return nwObj.getDouble(LAT_KEY);
		}
		return null;
	}
	
	public Double getNWLon() {
		JSONObject nwObj = record.getJSONObject(NW_POS_KEY);
		if (nwObj != null && nwObj.has(LON_KEY)) {
			return nwObj.getDouble(LON_KEY);
		}
		return null;
	}
	
	public Double getSELat() {
		JSONObject seObj = record.getJSONObject(SE_POS_KEY);
		if (seObj != null && seObj.has(LAT_KEY)) {
			return seObj.getDouble(LAT_KEY);
		}
		return null;
	}
	
	public Double getSELon() {
		JSONObject seObj = record.getJSONObject(SE_POS_KEY);
		if (seObj != null && seObj.has(LON_KEY)) {
			return seObj.getDouble(LON_KEY);
		}
		return null;
	}
	
	public boolean isBoundingBoxEmpty() {
		JSONObject nwPos = record.getJSONObject(NW_POS_KEY);
		JSONObject sePos = record.getJSONObject(SE_POS_KEY);
		return (nwPos == null || nwPos.isEmpty() || nwPos.isNullObject()) && 
			   (sePos == null || sePos.isEmpty() || sePos.isNullObject());
	}
	
	public String toString() {
		String result = (this.record != null) ? this.record.toString() : null;
		return result;
	}
}