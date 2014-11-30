/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.ntua.ece.cslab.modissense.queries.containers;

import java.nio.ByteBuffer;

/**
 *
 * @author Giannis Giannakopoulos
 */
public class UserIdStruct implements Serializable, Comparable<UserIdStruct> {

    private char snIdentifier;
    private Long id;

    public UserIdStruct() {
        
    }
    
    public UserIdStruct(char c, Long id) {
        this.snIdentifier = c;
        this.id = id;
    }
    
    public char getC() {
        return snIdentifier;
    }

    public void setC(char c) {
        this.snIdentifier = c;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
    
    
    @Override
    public void parseBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        this.snIdentifier = buffer.getChar();
        this.id = buffer.getLong();
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.putChar(this.snIdentifier);
        buffer.putLong(this.id);
        return buffer.array();
    }

    @Override
    public String toString() {
        return this.snIdentifier+""+this.id;
    }

    @Override
    public int compareTo(UserIdStruct o) {
        if(this.getC()<o.getC())
            return -1;
        else if(this.getC()>o.getC())
            return 1;
        else {
            if(this.getId()<o.getId())
                return -1;
            else if (this.getId() > o.getId()) {
                return 1;
            } else
                return 0;
        }
    }
}
