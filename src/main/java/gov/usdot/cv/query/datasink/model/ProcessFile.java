package gov.usdot.cv.query.datasink.model;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Loads a processes.xml file using the given uri.
 * 
 * Expecting the processes.xml file to be of the form.
 * 
 * <processes>
 *  <item>
 *      <number>1</number>
 *      <allocateInternetAddress>false</allocateInternetAddress>
 *      <id>i-829b0dd1</id>
 *      <group>mongodb.standalone</group>
 *      <publicDnsName>ec2-184-72-208-0.compute-1.amazonaws.com</publicDnsName>
 *      <privateDnsName>ip-10-118-15-133.ec2.internal</privateDnsName>
 *      <persistentDnsName>mongodb.dev-sdw.cv-dev.aws-dev.deleidos.com</persistentDnsName>
 *      <publicIpAddress>184.72.208.0</publicIpAddress>
 *      <privateIpAddress>10.118.15.133</privateIpAddress>
 *      <volumes>
 *          <id>vol-27976a62</id>
 *          <device>/dev/sdf1</device>
 *      </volumes>
 *      <volumes>
 *          <id>vol-94976ad1</id>
 *          <device>/dev/sdf2</device>
 *      </volumes>
 *      <launchTime>2014-05-28T19:12:10Z</launchTime>
 *  </item>
 *  ...
 */
public class ProcessFile {
	private static final String PROCESSES_PATH 					= "processes::";
	private static final String ITEM_PATH 						= "processes::item::";
	
	private static final String NUMBER_PATH 					= "processes::item::number::";
	private static final String ALLOCATE_INTERNET_ADDRESS_PATH 	= "processes::item::allocateInternetAddress::";
	private static final String ID_PATH 						= "processes::item::id::";
	private static final String GROUP_PATH 						= "processes::item::group::";
	private static final String PUBLIC_DNS_NAME_PATH 			= "processes::item::publicDnsName::";
	private static final String PRIVATE_DNS_NAME_PATH 			= "processes::item::privateDnsName::";
	private static final String PERSISTENT_DNS_NAME_PATH 		= "processes::item::persistentDnsName::";
	private static final String PUBLIC_IP_ADDRESS_PATH 			= "processes::item::publicIpAddress::";
	private static final String PRIVATE_IP_ADDRESS_PATH 		= "processes::item::privateIpAddress::";
	private static final String PERSISTENT_IP_ADDRESS_PATH 		= "processes::item::persistentIpAddress::";
	
	private static final String VOLUMES_PATH 					= "processes::item::volumes::";
	private static final String VOLUME_ID_PATH 					= "processes::item::volumes::id::";
	private static final String VOLUME_DEVICE_PATH 				= "processes::item::volumes::device::";
	
	private List<Item> items = new ArrayList<Item>();
	
	public ProcessFile(InputStream is) throws ParserConfigurationException, SAXException, IOException {
		parse(is);
	}

	private void parse(InputStream is) throws ParserConfigurationException, SAXException, IOException {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser parser = factory.newSAXParser();
		parser.parse(is, new FileHandler());
	}
	
	public List<Item> getAll() {
		return Collections.unmodifiableList(this.items);
	}
	
	public Item getItem(int number, String group) {
		for (Item item : items) {
			if (item.getNumber() == number && item.getGroup().equals(group)) {
				return item;
			}
		}
		return null;
	}
	
	private class FileHandler extends DefaultHandler {
		private Logger logger = Logger.getLogger(getClass());
		
		private LinkedList<String> path = new LinkedList<String>();
		private ArrayList<Volume> volumes = new ArrayList<Volume>();
		
		private Item.Builder iBuilder;
		private Volume.Builder vBuilder;
		private String value;

		public void startElement(
				String uri, 
				String localName, 
				String qName, 
				Attributes attributes) {
			path.addLast(qName);
			
			String currentPath = currentPath();
			
			if (currentPath.equals(PROCESSES_PATH)) {
				this.iBuilder = null;
				this.vBuilder = null;
				this.value = null;
			}
			
			if (currentPath.equals(ITEM_PATH)) {
				this.volumes = new ArrayList<Volume>();
				this.iBuilder = new Item.Builder();
				this.vBuilder = new Volume.Builder();
			}
			
			if (currentPath.equals(NUMBER_PATH) ||
				currentPath.equals(ALLOCATE_INTERNET_ADDRESS_PATH) ||
				currentPath.equals(ID_PATH) ||
				currentPath.equals(GROUP_PATH) || 
				currentPath.equals(PUBLIC_DNS_NAME_PATH) ||
				currentPath.equals(PRIVATE_DNS_NAME_PATH) ||
				currentPath.equals(PERSISTENT_DNS_NAME_PATH) ||
				currentPath.equals(PUBLIC_IP_ADDRESS_PATH) ||
				currentPath.equals(PRIVATE_IP_ADDRESS_PATH) ||
				currentPath.equals(PERSISTENT_IP_ADDRESS_PATH) ||
				currentPath.equals(VOLUME_ID_PATH) ||
				currentPath.equals(VOLUME_DEVICE_PATH)) {
				this.value = null;
			}
		}
		
		public void characters(
				char [] ch, 
				int start, 
				int length) {
			String tmp = new String(ch, start, length);
			this.value = tmp.trim();
		}
		
		public void endElement(
				String uri, 
				String localName, 
				String qName) {
			String currentPath = currentPath();
			
			if (currentPath.equals(ITEM_PATH) ) {
				this.iBuilder.setVolumes(this.volumes);
				items.add(this.iBuilder.build());
			}
			
			if (currentPath.equals(NUMBER_PATH)) {
				this.iBuilder.setNumber(Integer.parseInt(this.value));
			}
			
			if (currentPath.equals(ALLOCATE_INTERNET_ADDRESS_PATH)) {
				this.iBuilder.setAllocateInternetAddress(this.value);
			}
			
			if (currentPath.equals(ID_PATH)) {
				this.iBuilder.setId(this.value);
			}
			
			if (currentPath.equals(GROUP_PATH)) {
				this.iBuilder.setGroup(this.value);
			}
			
			if (currentPath.equals(PUBLIC_DNS_NAME_PATH)) {
				this.iBuilder.setPublicDnsName(this.value);
			}
			
			if (currentPath.equals(PRIVATE_DNS_NAME_PATH)) {
				this.iBuilder.setPrivateDnsName(this.value);
			}
			
			if (currentPath.equals(PERSISTENT_DNS_NAME_PATH)) {
				this.iBuilder.setPersistentDnsName(this.value);
			}
			
			if (currentPath.equals(PUBLIC_IP_ADDRESS_PATH)) {
				this.iBuilder.setPublicIpAddress(this.value);
			}

			if (currentPath.equals(PRIVATE_IP_ADDRESS_PATH)) {
				this.iBuilder.setPrivateIpAddress(this.value);
			}
			
			if (currentPath.equals(PERSISTENT_IP_ADDRESS_PATH)) {
				this.iBuilder.setPersistentIpAddress(this.value);
			}
			
			if (currentPath.equals(VOLUMES_PATH) ) {
				this.volumes.add(vBuilder.build());
			}
			
			if (currentPath.equals(VOLUME_ID_PATH)) {
				this.vBuilder.setId(this.value);
			}
			
			if (currentPath.equals(VOLUME_DEVICE_PATH)) {
				this.vBuilder.setDevice(this.value);
			}
			
			path.removeLast();
		}
		
		private String currentPath() {
			StringBuilder sb = new StringBuilder();
			for (String link : path) {
				sb.append(link).append("::");
			}
			return sb.toString();
		}
		
		@Override
		public void warning(SAXParseException e) throws SAXException {
			super.warning(e);
			logger.warn(e.toString(), e);
		}
		
		@Override
		public void fatalError(SAXParseException e) throws SAXException {
			super.fatalError(e);
			logger.fatal(e.toString(), e);
		}
		
		@Override
		public void error(SAXParseException e) throws SAXException {
			super.error(e);
			logger.error(e.toString(), e);
		}
	}
}