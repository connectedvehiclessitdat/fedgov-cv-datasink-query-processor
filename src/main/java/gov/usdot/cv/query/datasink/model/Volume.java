package gov.usdot.cv.query.datasink.model;

public class Volume {
	private String id;
	private String device;
	
	private Volume(
			String id, 
			String device) {
		this.id = id;
		this.device = device;
	}
	
	public String getId() 		{ return this.id; }
	public String getDevice() 	{ return this.device; }
	
	public static class Builder {
		private String id;
		private String device;
		
		public Builder setId(String id) {
			this.id = id;
			return this;
		}
		
		public Builder setDevice(String device) {
			this.device = device;
			return this;
		}
		
		public Volume build() {
			return new Volume(this.id, this.device);
		}
	}
}