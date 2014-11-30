package gr.ntua.ece.cslab.modissense.queries.data.generator;

import gr.ntua.ece.cslab.modissense.queries.containers.POI;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

/**
 *
 * Abstract class used to implement methods used by many Generator subclasses.
 *
 * @author Giannis Giannakopoulos
 */
public abstract class AbstractPopulator {

    protected String tableName;
    protected HTable table;
    protected Long numberOfUsers;
    protected Random random;
    protected LinkedList<POI> poiPool;
    protected Integer regionsPerSN = 3;

    public AbstractPopulator() {
        this.random = new Random();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getNumberOfUsers() {
        return numberOfUsers;
    }

    public void setNumberOfUsers(Long numberOfUsers) {
        this.numberOfUsers = numberOfUsers;
    }

    public Integer getRegionsPerSN() {
        return regionsPerSN;
    }

    public void setRegionsPerSN(Integer regionsPerSN) {
        this.regionsPerSN = regionsPerSN;
    }

    /**
     * Creates a table -- deletes the old one if exists.
     * @throws Exception 
     */
    protected void createTable() throws Exception {
        Configuration hbaseConf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);
        if (admin.tableExists(this.tableName)) {
            admin.disableTable(this.tableName);
            admin.deleteTable(this.tableName);
        }
        HTableDescriptor descriptor = new HTableDescriptor(this.tableName);
        descriptor.addFamily(new HColumnDescriptor("cf"));
        

        admin.createTable(descriptor, this.getSplitKeys(this.regionsPerSN));
        admin.close();
        this.table = new HTable(hbaseConf, this.tableName);
    }
    
    protected byte[][] getSplitKeys(int regionsPerSN) {
        byte[][] keys = new byte[(regionsPerSN-1)*3+3][10];
        List<UserIdStruct> userids = new LinkedList<>();
        userids.add(new UserIdStruct('F', 0l));
        for(int i=1;i<regionsPerSN;i++) 
            userids.add(new UserIdStruct('F', i*this.numberOfUsers/(regionsPerSN)));
        userids.add(new UserIdStruct('f', 0l));
        for(int i=1;i<regionsPerSN;i++) 
            userids.add(new UserIdStruct('f', i*this.numberOfUsers/(regionsPerSN)));
        for(int i=1;i<regionsPerSN;i++) 
            userids.add(new UserIdStruct('t', i*this.numberOfUsers/(regionsPerSN)));
        userids.add(new UserIdStruct('t', 0l));
        for(int i=0;i<keys.length;i++)
            keys[i] = userids.get(i).getBytes();
        return keys;
    }

    /**
     * Creates a POI pool to be assigned to the users by reading a CSV file using
     * openstreetmap POI format.
     * @param filename
     * @throws FileNotFoundException
     * @throws IOException 
     */
    protected void createPOIPoolByFile(String filename) throws FileNotFoundException, IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        poiPool = new LinkedList<>();
        Long i = 1l;
        while (reader.ready()) {
            String[] values = reader.readLine().split("\t");
            POI current = new POI();
            current.setName(values[4]);
            current.setX(new Double(values[3]));
            current.setY(new Double(values[2]));
            current.setId(i++);
            Set<String> keyws = new HashSet<>();
            int category = new Integer(values[0]);
            if(category<=7) {
                keyws.add("accommodation");
            } else if(category<=17) {
                keyws.add("amenity");
            } else if(category<=18) {
                keyws.add("barrier");
            } else if(category<=22) {
                keyws.add("education");
            } else if(category<=29) {
                keyws.add("food");
            } else if(category<=35) {
                keyws.add("health");
            } else if(category<=45) {
                keyws.add("landuse");
            } else if(category<=47) {
                keyws.add("money");
            } else if(category<=58) {
                keyws.add("pow");
            } else if(category<=72) {
                keyws.add("poi");
            } else if(category<=109) {
                keyws.add("shop");
            } else if(category<=136) {
                keyws.add("sport");
            } else if(category<=158) {
                keyws.add("tourist");
            } else if(category<=168) {
                keyws.add("transport");
            } else if(category<=171) {
                keyws.add("water");
            } else {
                keyws.add("unknown");
            }
            current.setKeywords(keyws);
            poiPool.add(current);
        }
        System.out.format("Parsed %d POIs from %s",poiPool.size(), filename);
    }

    /**
     * Creates the assignment, assuming that the user and POI pools have been
     * created.
     * @throws Exception 
     */
    protected abstract void assign() throws Exception;
}
