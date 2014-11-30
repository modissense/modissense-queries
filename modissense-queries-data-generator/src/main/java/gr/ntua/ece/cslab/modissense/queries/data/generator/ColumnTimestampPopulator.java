package gr.ntua.ece.cslab.modissense.queries.data.generator;

import gr.ntua.ece.cslab.modissense.queries.containers.POI;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Giannis Giannakopoulos
 */
public class ColumnTimestampPopulator extends AbstractPopulator {

    public ColumnTimestampPopulator() {
        super();
    }

    
    @Override
    protected void assign() throws Exception {
        List<Character> socialNetworks = new LinkedList<>();
        socialNetworks.add('F');
        socialNetworks.add('f');
        socialNetworks.add('t');
        for (char sn : socialNetworks) {
            List<Put> puts = new LinkedList<>();
            for (Long i = 1l; i <= this.numberOfUsers; i++) {
                List<POI> poilist = new LinkedList<>();
                int numberOfPoisVisited = (int) Math.floor(this.random.nextGaussian()*20 + 150);
                numberOfPoisVisited = (numberOfPoisVisited>0?numberOfPoisVisited:30);
                for (int j = 0; j < numberOfPoisVisited; j++) {
                    int poiIndex = this.random.nextInt(this.poiPool.size());
                    POI selected = this.poiPool.get(poiIndex);
                    POI forked = new POI();
                    forked.parseBytes(selected.getBytes());
                    forked.setScore(this.random.nextDouble());
                    forked.setTimestamp(System.currentTimeMillis() - this.random.nextInt(1000 * 3600 * 24 * 30 * 12));
                    poilist.add(forked);
                }
                UserIdStruct userId = new UserIdStruct(sn, i);
                Put put = new Put(userId.getBytes());
                for (POI p : poilist) {
                    put.add("cf".getBytes(), Bytes.toBytes(p.getTimestamp()), p.getBytes());
                }
                puts.add(put);
                if (i != 0 && i % 1000 == 0) {
                    System.out.print("Flushing 1000 users (offset " + i + ", SN: "+sn+")...\t");
                    this.table.put(puts);
                    puts.clear();
                    System.out.println("Done!");
                }
            }
            if (!puts.isEmpty()) {
                System.out.print("Flushing the last users (SN: "+sn+")...\t");
                this.table.put(puts);
                System.out.println("Done!");
            }
            puts.clear();
        }
        this.table.close();
    }
    
    public static void main(String[] args) throws Exception{
                if(args.length<3 ) {
            System.err.println("I need 3 arguments:\n"
                    + "\t1st arg is the POI file name\n"
                    + "\t2nd arg is the number of users to be created (per social network)\n"
                    + "\t3rd arg is the table name created to hbase\n"
                    + "\t4th (optional) number of regions per SN (default is 3)");
            System.exit(1);
        }
        Long users = new Long(args[1]);
        String fileName = args[0];
        String tableName = args[2];
        
        ColumnTimestampPopulator creator = new ColumnTimestampPopulator();
        creator.setNumberOfUsers(users);
        creator.setTableName(tableName);
        if(args.length==4) 
            creator.setRegionsPerSN(new Integer(args[3]));
        
        creator.createTable();
        creator.createPOIPoolByFile(fileName);
        creator.assign();

    }
}
