/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.ntua.ece.cslab.modissense.queries.clients.mr;

import gr.ntua.ece.cslab.modissense.queries.containers.POI;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author Giannis Giannakopoulos
 */
public class GeneralHotIntQueryMapper extends TableMapper<LongWritable, HotnessInterestWritable>{

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context); 
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        for(Map.Entry<byte[], byte[]> e : value.getFamilyMap("cf".getBytes()).entrySet()) {
            POI p  = new POI();
            p.parseBytes(e.getValue());
            context.write(new LongWritable(p.getId()), new HotnessInterestWritable(1, p.getScore()));
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context); 
    }
    
}
