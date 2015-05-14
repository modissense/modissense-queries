/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.ntua.ece.cslab.modissense.queries.clients;

import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Giannis Giannakopoulos
 */
public class GetPOIClient {

    public GetPOIClient() {
    }
    
    
    /**
     * Returns the number of comments (globally, for all users), for a poi based 
     * 
     * @param poiId
     * @return 
     */
    public int getNumberOfComments(Long poiId) throws IOException {
        HTable table = new HTable(HBaseConfiguration.create(), "TextRepo");
        byte[] startRow = new byte[Long.SIZE*2+1], stopRow=new byte[Long.SIZE*2+1];
        UserIdStruct start = new UserIdStruct('F', Long.MIN_VALUE);
        UserIdStruct stop = new UserIdStruct('t', Long.MAX_VALUE);
        int index=0;
        for (byte b:Bytes.toBytes(poiId)) {
            startRow[index++] = b;
        }
        for (byte b:start.getBytes()) {
            startRow[index++] = b;
        }
        index=0;
        for (byte b:Bytes.toBytes(poiId)) {
            stopRow[index++] = b;
        }
        for (byte b:stop.getBytes()) {
            stopRow[index++] = b;
        }
        
        Scan scan = new Scan(startRow, stopRow);
        ResultScanner resScan= table.getScanner(scan);
        int results = 0;
        while(resScan.next()!=null) {
            results++;
        }
        return results;
    }
    
    
    public static void main(String[] args) {
        GetPOIClient client = new GetPOIClient();
        
        System.out.println("Results:" + client.getNumberOfComments(new Long(args[0])));
    }
    
}
