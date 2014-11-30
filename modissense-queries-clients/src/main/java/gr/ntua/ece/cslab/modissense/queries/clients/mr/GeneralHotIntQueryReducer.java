/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.ntua.ece.cslab.modissense.queries.clients.mr;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author giannis
 */
public class GeneralHotIntQueryReducer extends TableReducer<LongWritable, HotnessInterestWritable, ImmutableBytesWritable>{

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context); 
    }

    @Override
    protected void reduce(LongWritable key, Iterable<HotnessInterestWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<HotnessInterestWritable> it = values.iterator();
        Integer hotness=0;
        Double interest = 0d;
        while(it.hasNext()) {
            HotnessInterestWritable c = it.next();
            hotness += c.getHotness();
            interest+= c.getInterest();
        }
        Put put = new Put(Bytes.toBytes(key.get()));
        put.add("cf".getBytes(), "hotness".getBytes(), Bytes.toBytes(hotness));
        put.add("cf".getBytes(), "interest".getBytes(), Bytes.toBytes(interest));
        
        context.write(null, put);

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }
    
}
