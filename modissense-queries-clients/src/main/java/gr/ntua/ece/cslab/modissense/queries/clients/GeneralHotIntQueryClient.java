/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.ntua.ece.cslab.modissense.queries.clients;

import gr.ntua.ece.cslab.modissense.queries.clients.mr.GeneralHotIntQueryCombiner;
import gr.ntua.ece.cslab.modissense.queries.clients.mr.GeneralHotIntQueryReducer;
import gr.ntua.ece.cslab.modissense.queries.clients.mr.GeneralHotIntQueryMapper;
import gr.ntua.ece.cslab.modissense.queries.clients.mr.HotnessInterestWritable;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Launches a new MR job in order to estimate the non personalized interest and
 * hotness of each POI. The accepted parameters are mapped to the timestamps.
 *
 * @author Giannis Giannakopoulos
 */
public class GeneralHotIntQueryClient extends AbstractQueryClient {

    private Long startTimestamp = 0l;
    private Long endTimestamp = Long.MAX_VALUE;
    private int numOfTasks = 4;
    private String srcTable, targetTable;
    private boolean dropTableIfExists=false;

    public GeneralHotIntQueryClient() {

    }

    public String getSrcTable() {
        return srcTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
        this.targetTable = this.srcTable + "-hi-" + this.startTimestamp + "-" + this.endTimestamp;
    }

    public int getNumOfTasks() {
        return numOfTasks;
    }

    public void setNumOfTasks(int numOfTasks) {
        this.numOfTasks = numOfTasks;
    }

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(Long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(Long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public boolean isDropTableIfExists() {
        return dropTableIfExists;
    }

    public void setDropTableIfExists(boolean dropTableIfExists) {
        this.dropTableIfExists = dropTableIfExists;
    }

    /**
     * True if the table exists, else false
     *
     * @return
     */
    private boolean createIfNotExist() {
        try {
            String targetTableName = this.targetTable;
            HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
            if (admin.tableExists(targetTableName) && !this.dropTableIfExists) {
                admin.close();
                return false;
            } else {
                HTableDescriptor desc = new HTableDescriptor(targetTableName);
                desc.addFamily(new HColumnDescriptor("cf"));
                admin.createTable(desc, Bytes.toBytes(0l), Bytes.toBytes(10000l), numOfTasks);
                admin.close();
            }
        } catch (IOException ex) {
            Logger.getLogger(GeneralHotIntQueryClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }

    @Override
    public void executeQuery() {
        try {
            if (this.createIfNotExist()) {      //table exists            
                Configuration conf = HBaseConfiguration.create();
                Job job = new Job(conf, "Non personalized hotness interest");
                job.setJarByClass(GeneralHotIntQueryClient.class);
                Scan scan = new Scan();
                scan.setCaching(10000);

                scan.setFilter(new ColumnRangeFilter(Bytes.toBytes(startTimestamp), true, Bytes.toBytes(endTimestamp), true));
                TableMapReduceUtil.initTableMapperJob(
                        this.srcTable, // table name in bytes
                        scan, // scanner to use
                        GeneralHotIntQueryMapper.class, // mapper class
                        LongWritable.class, // key class
                        HotnessInterestWritable.class, // value class
                        job);                               // job object

                TableMapReduceUtil.initTableReducerJob(
                        this.targetTable,
                        GeneralHotIntQueryReducer.class,
                        job);
                job.setPartitionerClass(HashPartitioner.class);
                job.setCombinerClass(GeneralHotIntQueryCombiner.class);
                job.setNumReduceTasks(4);
                job.setOutputFormatClass(TableOutputFormat.class);

                job.waitForCompletion(true);
            }
            this.openConnection(targetTable);
        } catch (IOException | InterruptedException | ClassNotFoundException ex) {
            Logger.getLogger(GeneralHotIntQueryClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public double getInterest(Long poiId) {
        try {
            Result res = this.table.get(new Get(Bytes.toBytes(poiId)));
            return Bytes.toDouble(res.getValue("cf".getBytes(), "interest".getBytes()));
        } catch (IOException ex) {
            Logger.getLogger(GeneralHotIntQueryClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        return -1d;
    }

    public int getHotness(Long poiId) {
        try {
            Result res = this.table.get(new Get(Bytes.toBytes(poiId)));
            return Bytes.toInt(res.getValue("cf".getBytes(), "hotness".getBytes()));
        } catch (IOException ex) {
            Logger.getLogger(GeneralHotIntQueryClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        return -1;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("I expect the table name!");
            System.exit(1);
        }
        GeneralHotIntQueryClient query = new GeneralHotIntQueryClient();
        query.setSrcTable(args[0]);
        query.executeQuery();
        
        System.out.println(query.getHotness(1l)+", "+query.getInterest(1l));
    }

}
