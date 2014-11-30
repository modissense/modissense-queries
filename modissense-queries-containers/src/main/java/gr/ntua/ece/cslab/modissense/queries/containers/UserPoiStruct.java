package gr.ntua.ece.cslab.modissense.queries.containers;

import java.nio.ByteBuffer;

/**
 * 
 * @author giagulei
 *
 */
public class UserPoiStruct {
	
	private char snIdentifier;
	private Long poiID;
    private Long userID;

    public UserPoiStruct() {}
    
    public UserPoiStruct(char c, Long userId,Long poiID) {
        this.snIdentifier = c;
        this.userID = userId;
        this.poiID = poiID;
    }
    
    public char getC() {
        return snIdentifier;
    }

    public void setC(char c) {
        this.snIdentifier = c;
    }

    public Long getUserId() {
        return userID;
    }

    public void setUserId(Long id) {
        this.userID = id;
    }
    
    public Long getPoiId() {
        return poiID;
    }

    public void setPoiId(Long id) {
        this.poiID = id;
    }
    
    public void parseBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        this.snIdentifier = buffer.getChar();
        this.userID = buffer.getLong();
        this.poiID = buffer.getLong();
    }

    public byte[] getBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(2*Long.SIZE/8+Character.SIZE/8);
        buffer.putChar(this.snIdentifier);
        buffer.putLong(this.userID);
        buffer.putLong(this.poiID);
        return buffer.array();
    }

    @Override
    public String toString() {
        return this.snIdentifier+""+this.userID+""+this.poiID;
    }

    public int compareTo(UserPoiStruct o) {
    	if(this.getC()<o.getC())
    		return -1;
    	else if (this.getC()>o.getC())
    		return 1;
    	else if(this.getUserId()<o.getUserId())
            return -1;
        else if(this.getUserId()>o.getUserId())
            return 1;
        else {
            if(this.getPoiId()<o.getPoiId())
                return -1;
            else if (this.getPoiId()>o.getPoiId()) {
                return 1;
            } else
                return 0;
        }
    }

}
