/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.ntua.ece.cslab.modissense.queries.clients.mr;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author giannis
 */
public class GeneralHotIntQueryCombiner extends Reducer<LongWritable, HotnessInterestWritable, LongWritable, HotnessInterestWritable>{

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
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
        
        context.write(key, new HotnessInterestWritable(hotness, interest));

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context); //To change body of generated methods, choose Tools | Templates.
    }
    
}
