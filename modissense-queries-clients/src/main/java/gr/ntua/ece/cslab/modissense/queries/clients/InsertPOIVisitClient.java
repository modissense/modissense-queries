package gr.ntua.ece.cslab.modissense.queries.clients;

import gr.ntua.ece.cslab.modissense.queries.containers.POI;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is used to add a new user checkin to a specified POI. 
 * @author Giannis Giannakopoulos
 */
public class InsertPOIVisitClient extends AbstractQueryClient {

    private UserIdStruct userId;
    private POI poi;

    public UserIdStruct getUserId() {
        return userId;
    }

    public void setUserId(UserIdStruct userId) {
        this.userId = userId;
    }

    public POI getPoi() {
        return poi;
    }

    public void setPoi(POI poi) {
        this.poi = poi;
    }
    
    /**
     * Insert a new visit into the table. This is not thread safe!
     */
    @Override
    public void executeQuery() {
        try {
            Put put = new Put(this.userId.getBytes());
            put.add("cf".getBytes(), Bytes.toBytes(this.poi.getTimestamp()), this.poi.getBytes());
            this.table.put(put);
            
        } catch (IOException ex) {
            Logger.getLogger(InsertPOIVisitClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
