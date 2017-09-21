package gov.usdot.cv.query.datasink.validator;

import gov.usdot.cv.common.model.BoundingBox;
import gov.usdot.cv.query.datasink.exception.QueryProcessorException;

public class BoundingBoxValidator {
	
	private BoundingBoxValidator() {
		// All method invocation goes through static methods
	}
	
	public static BoundingBox validate(BoundingBox bb) throws QueryProcessorException {
		if (bb == null) {
			throw new QueryProcessorException("BoundingBox object is null.");
		}
		
		if (bb.getNWLat() == null) {
			throw new QueryProcessorException("NW latitude is not set.");
		}
		
		if (bb.getNWLon() == null) {
			throw new QueryProcessorException("NW longitude is not set.");
		}
		
		if (bb.getSELat() == null) {
			throw new QueryProcessorException("SE latitude is not set.");
		}
		
		if (bb.getSELon() == null) {
			throw new QueryProcessorException("SE longitude is not set.");
		}
		
		if (bb.getNWLat() > BoundingBox.MAX_LAT || bb.getNWLat() < BoundingBox.MIN_LAT) {
			throw new QueryProcessorException("Invalid NW latitude value.");
		}
		
		if (bb.getNWLon()> BoundingBox.MAX_LON || bb.getNWLon() < BoundingBox.MIN_LON) {
			throw new QueryProcessorException("Invalid NW longitude value.");
		}
		
		if (bb.getSELat() > BoundingBox.MAX_LAT || bb.getSELat() < BoundingBox.MIN_LAT) {
			throw new QueryProcessorException("Invalid SE latitude value.");
		}
		
		if (bb.getSELon() > BoundingBox.MAX_LON || bb.getSELon() < BoundingBox.MIN_LON) {
			throw new QueryProcessorException("Invalid SE longitude value.");
		}
		
		if (bb.getNWLat() < bb.getSELat() || bb.getNWLon() > bb.getSELon()) {
			throw new QueryProcessorException("NW and SE positions doesn't form a bounding box.");
		}
		
		return bb;
	}
	
}