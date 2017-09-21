package gov.usdot.cv.query.datasink.model;

import java.util.Collections;
import java.util.List;

public class Item {
	private int number;
	private String id;
	private String group;
	private String publicDnsName;
	private String privateDnsName;
	private String persistentDnsName;
	private String publicIpAddress;
	private String privateIpAddress;
	private String persistentIpAddress;
	private boolean allocateInternetAddress;
	private String launchTime;
	private List<Volume> volumes;
	
	private Item(
			int number, 
			String id, 
			String group, 
			String publicDnsName, 
			String privateDnsName,
			String persistentDnsName, 
			String publicIpAddress, 
			String privateIpAddress,
			String persistentIpAddress,
			boolean allocateInternetAddress,
			String launchTime,
			List<Volume> volumes) {
		this.number = number;
		this.id = id;
		this.group = group;
		this.publicDnsName = publicDnsName;
		this.privateDnsName = privateDnsName;
		this.persistentDnsName = persistentDnsName;
		this.publicIpAddress = publicIpAddress;
		this.privateIpAddress = privateIpAddress;
		this.persistentIpAddress = persistentIpAddress;
		this.allocateInternetAddress = allocateInternetAddress;
		this.launchTime = launchTime;
		this.volumes = volumes;
	}
	
	public int getNumber() 						{ return this.number; }
	public String getId() 						{ return this.id; }
	public String getGroup() 					{ return this.group; }
	public String getPublicDnsName() 			{ return this.publicDnsName; }
	public String getPrivateDnsName() 			{ return this.privateDnsName; }
	public String getPersistentDnsName()		{ return this.persistentDnsName; }
	public String getPublicIpAddress() 			{ return this.publicIpAddress; }
	public String getPrivateIpAddress() 		{ return this.privateIpAddress; }
	public String getPersistentIpAddress()		{ return this.persistentIpAddress; }
	public boolean getAllocateInternetAddress() { return this.allocateInternetAddress; }
	public String getLaunchTime() 				{ return this.launchTime; }
	public List<Volume> getVolumes()			{ return this.volumes; }
	
	public static class Builder {
		private int number = -1;
		private String id;
		private String group;
		private String publicDnsName;
		private String privateDnsName;
		private String persistentDnsName;
		private String publicIpAddress;
		private String privateIpAddress;
		private String persistentIpAddress;
		private boolean allocateInternetAddress;
		private String launchTime;
		private List<Volume> volumes;
		
		public Builder setNumber(int number) {
			this.number = number;
			return this;
		}
		
		public Builder setId(String id) {
			this.id = id;
			return this;
		}
		
		public Builder setGroup(String group) {
			this.group = group;
			return this;
		}
		
		public Builder setPublicDnsName(String publicDnsName) {
			this.publicDnsName = publicDnsName;
			return this;
		}
		
		public Builder setPrivateDnsName(String privateDnsName) {
			this.privateDnsName = privateDnsName;
			return this;
		}
		
		public Builder setPersistentDnsName(String persistentDnsName) {
			this.persistentDnsName = persistentDnsName;
			return this;
		}
		
		public Builder setPublicIpAddress(String publicIpAddress) {
			this.publicIpAddress = publicIpAddress;
			return this;
		}
		
		public Builder setPrivateIpAddress(String privateIpAddress) {
			this.privateIpAddress = privateIpAddress;
			return this;
		}
		
		public Builder setPersistentIpAddress(String persistentIpAddress) {
			this.persistentIpAddress = persistentIpAddress;
			return this;
		}
		
		public Builder setAllocateInternetAddress(String allocateInternetAddress) {
			this.allocateInternetAddress = Boolean.valueOf(allocateInternetAddress);
			return this;
		}
		
		public Builder setLaunchTime(String launchTime) {
			this.launchTime = launchTime;
			return this;
		}
		
		public Builder setVolumes(List<Volume> volumes) {
			if (volumes != null) {
				this.volumes = Collections.unmodifiableList(volumes);
			}
			return this;
		}
		
		public Item build() {
			return new Item(
				this.number, 
				this.id, 
				this.group, 
				this.publicDnsName, 
				this.privateDnsName,
				this.persistentDnsName, 
				this.publicIpAddress, 
				this.privateIpAddress,
				this.persistentIpAddress,
				this.allocateInternetAddress,
				this.launchTime,
				this.volumes);
		}
	}
	
}