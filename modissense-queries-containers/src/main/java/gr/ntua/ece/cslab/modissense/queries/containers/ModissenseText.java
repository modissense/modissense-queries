package gr.ntua.ece.cslab.modissense.queries.containers;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Date;

/**
 * 
 * @author giagulei
 *
 */
public class ModissenseText implements Serializable{

	private String text;
	private Date timestamp;
	
	public ModissenseText(){}
	
	public String getText(){
		return text;
	}
	
	public void setText(String text){
		this.text = text;
	}
	
	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
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

	}

}
