/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.ntua.ece.cslab.modissense.queries.clients.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Giannis Giannakopoulos
 */
public class HotnessInterestWritable implements Writable {
    
    private int hotness;
    private double interest;

    public HotnessInterestWritable() {
    
    }

    public HotnessInterestWritable(int hotness, double interest) {
        this.hotness = hotness;
        this.interest = interest;
    }
    
    public int getHotness() {
        return hotness;
    }

    public void setHotness(int hotness) {
        this.hotness = hotness;
    }

    public double getInterest() {
        return interest;
    }

    public void setInterest(double interest) {
        this.interest = interest;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(hotness);
        d.writeDouble(interest);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.hotness = di.readInt();
        this.interest = di.readDouble();
    }

    @Override
    public String toString() {
        return this.hotness+", "+this.interest;
    }
    
    
}
