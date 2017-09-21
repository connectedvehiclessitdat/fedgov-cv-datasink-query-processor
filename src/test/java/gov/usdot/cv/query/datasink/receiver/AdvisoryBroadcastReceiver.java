package gov.usdot.cv.query.datasink.receiver;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class AdvisoryBroadcastReceiver implements Runnable {
	private BlockingQueue<ByteBuffer> queue;
	private DatagramSocket socket;
	
	public AdvisoryBroadcastReceiver(
			BlockingQueue<ByteBuffer> queue, 
			int port) throws SocketException {
		this.queue = queue;
		this.socket = new DatagramSocket(port);
	}
	
	public void run() {
		System.out.println("Starting advistory broadcast receiver ...");
		 while (true) try {
			byte [] buffer = new byte[10000];
			DatagramPacket dp = new DatagramPacket(buffer, buffer.length);
			this.socket.receive(dp);
			if (this.queue != null) {
				this.queue.offer(ByteBuffer.wrap(buffer));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}