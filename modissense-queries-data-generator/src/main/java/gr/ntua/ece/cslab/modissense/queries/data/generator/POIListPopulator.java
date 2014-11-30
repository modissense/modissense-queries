/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.ntua.ece.cslab.modissense.queries.data.generator;

import gr.ntua.ece.cslab.modissense.queries.containers.POI;
import gr.ntua.ece.cslab.modissense.queries.containers.POIList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

@Deprecated
/**
 * 
 * This class creates a random assignment of POIs, given to specific users, by
 * creating POILists for each user.
 * @author Giannis Giannakopoulos
 */
public class POIListPopulator extends AbstractPopulator {
//        protected static String[] keywords = {"dance", "food", "wine", "fun", "restaurant", "bar", "pleasure", "drink", "alcohol"};
    

    public POIListPopulator() {
        super();
    }
    
    /**
     *
     * @throws IOException
     */
    @Override
    protected void assign() throws IOException {
        List<Put> puts = new LinkedList<>();
        for(Long i=1l; i<=this.numberOfUsers; i++) {
            POIList poilist = new POIList();
            System.out.print("Working for user "+i+"\t");
            for(int j=0;j<100;j++) {
                int poiIndex = this.random.nextInt(this.poiPool.size());
                POI selected = this.poiPool.get(poiIndex);
                POI forked = new POI();
                forked.parseBytes(selected.getBytes());
                forked.setScore(this.random.nextDouble());
                poilist.addPOI(forked);
            }
            Put put = new Put(Bytes.toBytes(i));
            put.add("cf".getBytes(), "".getBytes(), poilist.getCompressedBytes());
            puts.add(put);
            System.out.println("Done!");
            if(i!=0 && i%1000==0) {
                this.table.put(puts);
                puts.clear();
            }
        }
        if(!puts.isEmpty())
            this.table.put(puts);
        puts.clear();
        this.table.close();
    }
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        if(args.length<3 ) {
            System.err.println("I need 3 arguments:\n"
                    + "\t1st arg is the POI file name\n"
                    + "\t2nd arg is the number of users to be created\n"
                    + "\t3rd arg is the table name created to hbase.");
            System.exit(1);
        }
        Long users = new Long(args[1]);
        String fileName = args[0];
        String tableName = args[2];
        
        POIListPopulator creator = new POIListPopulator();
        creator.createPOIPoolByFile(fileName);
        creator.setNumberOfUsers(users);
        creator.setTableName(tableName);
        creator.createTable();
        creator.assign();
    }

}
