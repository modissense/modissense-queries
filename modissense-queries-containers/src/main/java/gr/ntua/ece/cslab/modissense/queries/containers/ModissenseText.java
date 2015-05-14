package gr.ntua.ece.cslab.modissense.queries.containers;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * 
 * @author giagulei
 *
 */
public class ModissenseText implements Serializable{

	private String text;
	private long timestamp;
	
	public ModissenseText(){}
	
	public String getText(){
		return text;
	}
	
	public void setText(String text){
		this.text = text;
	}
	
	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void parseBytes(byte[] bytes) throws UnsupportedEncodingException {
		
		int index = 0;
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		
		int sizeOfText = buffer.getInt(index);
		index+= Integer.SIZE/8;
		
		this.text = new String(bytes, index, sizeOfText);
		index+=this.text.getBytes("UTF-8").length;
			
	}

	
	public byte[] getBytes() throws UnsupportedEncodingException {
		
		int totalSize = Integer.SIZE/8 + text.getBytes("UTF-8").length;
		
		byte[] serializable = new byte[totalSize];
		
		ByteBuffer buffer = ByteBuffer.wrap(serializable);
		
		buffer.putInt(this.text.getBytes("UTF-8").length);
		buffer.put(this.text.getBytes("UTF-8"));
		
		return serializable;
	}
	
	public static void main(String[] args) {
		ModissenseText mtext = new ModissenseText();
		mtext.setText("This is a text");
		mtext.setTimestamp(123456789L);
		
		System.out.println("Date = "+mtext.getTimestamp());
	}

}
