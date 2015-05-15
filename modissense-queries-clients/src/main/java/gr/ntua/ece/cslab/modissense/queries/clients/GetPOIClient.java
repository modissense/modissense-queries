/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.ntua.ece.cslab.modissense.queries.clients;

import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Giannis Giannakopoulos
 */
public class GetPOIClient {
    
    private final String TABLE_NAME_TEXT_REPO="TextRepo";

    public GetPOIClient() {
    }
    
    
    /**
     * Returns the number of comments (globally, for all users) for a given poi.
     * @param poiId 
     * @return the number of comments made
     * @throws java.io.IOException 
     */
    public int getNumberOfComments(Long poiId) throws IOException {
        HTable table = new HTable(HBaseConfiguration.create(), TABLE_NAME_TEXT_REPO);
        Scan scan = new Scan(Bytes.toBytes(poiId),Bytes.toBytes(poiId+1));
        int results;
        try (ResultScanner resScan = table.getScanner(scan)) {
            results = 0;
            while(resScan.next()!=null) {
                results++;
            }
        }
        System.out.println("Number of global comments for POI "+poiId+": "+results);
        return results;
    }
    
    public String getCommentFromFriend(Long poiId, UserIdStruct friend){
        return "";
    }
    
    
    public static void main(String[] args) throws IOException {
        GetPOIClient client = new GetPOIClient();
        
        System.out.println("Results:" + client.getNumberOfComments(new Long(args[0])));
    }
    
}
