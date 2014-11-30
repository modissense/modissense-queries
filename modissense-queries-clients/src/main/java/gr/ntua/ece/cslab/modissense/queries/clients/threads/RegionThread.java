/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.ntua.ece.cslab.modissense.queries.clients.threads;

import gr.ntua.ece.cslab.modissense.queries.containers.POIList;
import gr.ntua.ece.cslab.modissense.queries.containers.UserCheckinsQueryArguments;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import gr.ntua.ece.cslab.modissense.queries.coprocessors.ColumnIndexProtocol;
import gr.ntua.ece.cslab.modissense.queries.coprocessors.POIListProtocol;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Thread object used to hit a specific region thread.
 * @author Giannis Giannakopoulos
 */
public class RegionThread  extends Thread {

    private UserCheckinsQueryArguments arguments;
    private long executionTime;
    private POIList results;
    private UserIdStruct firstKeyOfRegion;
    private HTable table;
    private Class<?extends CoprocessorProtocol> protocol;
    
    public RegionThread() {
        super();
    }

    public UserCheckinsQueryArguments getArguments() {
        return arguments;
    }

    public void setArguments(UserCheckinsQueryArguments arguments) {
        this.arguments = arguments;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public POIList getResults() {
        return results;
    }

    public UserIdStruct getFirstKeyOfRegion() {
        return firstKeyOfRegion;
    }

    public void setFirstKeyOfRegion(UserIdStruct firstKeyOfRegion) {
        this.firstKeyOfRegion = firstKeyOfRegion;
    }

    public HTable getTable() {
        return table;
    }

    public void setTable(HTable table) {
        this.table = table;
    }

    public void setProtocol(Class<? extends CoprocessorProtocol> protocol) {
        this.protocol = protocol;
    }
    
    @Override
    public synchronized void start() {
        super.start(); 
    }
    
    @Override
    public void run() {
        this.executionTime = System.currentTimeMillis();
        if(this.protocol.toString().equals(ColumnIndexProtocol.class.toString())){
            ColumnIndexProtocol prot = this.table.coprocessorProxy(ColumnIndexProtocol.class, this.firstKeyOfRegion.getBytes());
            try {
                this.results = new POIList();
                this.results.parseCompressedBytes(prot.getPOIs(this.arguments));
            } catch (IOException ex) {
                Logger.getLogger(RegionThread.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        } else if(this.protocol.toString().equals(POIListProtocol.class.toString())) {
            POIListProtocol prot = this.table.coprocessorProxy(POIListProtocol.class, this.firstKeyOfRegion.getBytes());
            try {
                this.results = new POIList();
                this.results.parseCompressedBytes(prot.getPOIs(this.arguments));
            } catch (IOException ex) {
                Logger.getLogger(RegionThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            System.err.println("Don't know what to do!!!");
            System.exit(1);
        }
        
        this.executionTime = System.currentTimeMillis()-this.executionTime;
    }
    
}
