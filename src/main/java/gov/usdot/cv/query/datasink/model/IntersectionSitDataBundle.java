package gov.usdot.cv.query.datasink.model;

import gov.usdot.asn1.generated.j2735.semi.IntersectionRecord;
import gov.usdot.asn1.generated.j2735.semi.IntersectionSituationData;

public class IntersectionSitDataBundle {
	
	private IntersectionSituationData intersectionSitData;
	
	private IntersectionSitDataBundle(
			IntersectionSituationData intersectionSitData) {
		this.intersectionSitData = intersectionSitData;
	}
	
	public IntersectionRecord getIntersectionRecord() {
		return intersectionSitData.getIntersectionRecord();
	}
	
	public static class Builder {
		private IntersectionSituationData intersectionSitData;
		
		public Builder setIntersectionSitData(IntersectionSituationData intersectionSitData) {
			this.intersectionSitData = intersectionSitData;
			return this;
		}
		
		public IntersectionSitDataBundle build() {
			if (this.intersectionSitData != null) {
				return new IntersectionSitDataBundle(this.intersectionSitData);
			}
			return null;
		}
	}
	
}