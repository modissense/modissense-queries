package gr.ntua.ece.cslab.modissense.queries.clients;

import gr.ntua.ece.cslab.modissense.queries.clients.threads.RegionThread;
import gr.ntua.ece.cslab.modissense.queries.containers.POI;
import gr.ntua.ece.cslab.modissense.queries.containers.POIList;
import gr.ntua.ece.cslab.modissense.queries.containers.UserCheckinsQueryArguments;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import gr.ntua.ece.cslab.modissense.queries.coprocessors.ColumnIndexProtocol;
import gr.ntua.ece.cslab.modissense.queries.coprocessors.POIListProtocol;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.HRegionLocation;

/**
 *
 * @author Giannis Giannakopoulos
 */
public class UserCheckinsQueryClient extends AbstractQueryClient {

    private UserCheckinsQueryArguments arguments;
    private POIList results;
    private Class protocol = ColumnIndexProtocol.class;
    private long executionTime;
    private boolean orderByInterest = true;

    public UserCheckinsQueryClient() {
        super();
//        System.out.println("Initializing UserCheckinsQueryClient");
    }

    public UserCheckinsQueryArguments getArguments() {
        return arguments;
    }

    public void setArguments(UserCheckinsQueryArguments arguments) {
        this.arguments = arguments;
    }

    public POIList getResults() {
        return results;
    }

    public Class getProtocol() {
        return protocol;
    }

    /**
     * Default protocol is ColumnIndexProtocol, specify another one if needs be.
     *
     * @param protocol
     */
    public void setProtocol(Class protocol) {
        this.protocol = protocol;
    }

    public long getExecutionTime() {
        return this.executionTime;
    }

    public void executeSerializedQuery() {
        List<POIList> intermediateResults = new LinkedList<>();
        for (UserIdStruct l : this.getRegionsKeys()) {

            if (this.protocol.toString().equals(ColumnIndexProtocol.class.toString())) {
                ColumnIndexProtocol prot = this.table.coprocessorProxy(ColumnIndexProtocol.class, l.getBytes());
                try {
                    POIList resultsLocal = new POIList();
                    resultsLocal.parseCompressedBytes(prot.getPOIs(this.arguments));
                    intermediateResults.add(resultsLocal);
                } catch (IOException ex) {
                    Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                }

            } else {
                System.err.println("Don't know what to do!!!");
                System.exit(1);
            }

            this.executionTime = System.currentTimeMillis() - this.executionTime;
        }

        this.mergeResults(intermediateResults);
        this.executionTime = System.currentTimeMillis() - this.executionTime;

//        try {
//            List<RegionThread> threads = new LinkedList<>();
//            for (UserIdStruct l : this.getRegionsKeys()) {
//                RegionThread thread = new RegionThread();
//                if(this.classloader!=null) {
//                    thread.setContextClassLoader(this.classloader);
//                    Enumeration<URL> en = thread.getContextClassLoader().getResources("");
//                    while(en.hasMoreElements()) {
//                        System.out.println(en.nextElement().toString());
//                    }
//                }
//                thread.setFirstKeyOfRegion(l);
//                thread.setProtocol(this.protocol);
//                thread.setArguments(arguments);
//                thread.setTable(this.table);
//                threads.add(thread);
//            }
//            
//            for(Thread t : threads)
//                t.start();
//            
//            List<POIList> intermediateResults = new LinkedList<>();
//            for(RegionThread t: threads) {
//                t.join();
//                intermediateResults.add(t.getResults());
//            }
//            
//            this.mergeResults(intermediateResults);
//            this.executionTime = System.currentTimeMillis() - this.executionTime;
//            
//        } catch (InterruptedException ex) {
//            Logger.getLogger(UserCheckinsQueryClient.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (IOException ex) {
//            Logger.getLogger(UserCheckinsQueryClient.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }

    @Override
    public void executeQuery() {
        this.executionTime = System.currentTimeMillis();
//        System.out.println("Setting the correct classloader");
        try {
            List<RegionThread> threads = new LinkedList<>();
            for (UserIdStruct l : this.getRegionsKeys()) {
                RegionThread thread = new RegionThread();
                thread.setFirstKeyOfRegion(l);
                thread.setProtocol(this.protocol);
                thread.setArguments(arguments);
                thread.setTable(this.table);
                threads.add(thread);
            }

            for (Thread t : threads) {
                t.start();
            }

            List<POIList> intermediateResults = new LinkedList<>();
            for (RegionThread t : threads) {
                t.join();
                intermediateResults.add(t.getResults());
            }

            this.mergeResults(intermediateResults);
            this.executionTime = System.currentTimeMillis() - this.executionTime;

        } catch (InterruptedException ex) {
            Logger.getLogger(UserCheckinsQueryClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private List<UserIdStruct> getRegionsKeys() {
        try {
            Collections.sort(arguments.getUserIds());
            byte[] firstKey = this.arguments.getUserIds().get(0).getBytes(),
                    lastKey = this.arguments.getUserIds().get(this.arguments.getUserIds().size() - 1).getBytes();
            List<HRegionLocation> regionsInRange = this.table.getRegionsInRange(firstKey, lastKey);
            List<UserIdStruct> regionKeys = new LinkedList<>();
            for (HRegionLocation loc : regionsInRange) {
                byte[] startKey = loc.getRegionInfo().getStartKey();
                UserIdStruct current = new UserIdStruct();
                current.parseBytes(startKey);
                regionKeys.add(current);
            }
            return regionKeys;
        } catch (IOException ex) {
            Logger.getLogger(UserCheckinsQueryClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private void mergeResults(List<POIList> intermediateResults) {
        HashMap<POI, List<Double>> scores = new HashMap<>();
        for (POIList l : intermediateResults) {
            for (POI p : l.getPOIs()) {
                if (!scores.containsKey(p)) {
                    scores.put(p, new LinkedList<Double>());
                }
                scores.get(p).add(p.getScore());
            }
        }

        List<POI> tempResults = new LinkedList<>();
        for (Map.Entry<POI, List<Double>> e : scores.entrySet()) {
            POI finalPoi = new POI();
            finalPoi.setId(e.getKey().getId());
            finalPoi.setKeywords(e.getKey().getKeywords());
            finalPoi.setName(e.getKey().getName());
            finalPoi.setX(e.getKey().getX());
            finalPoi.setY(e.getKey().getY());
            double sum = 0.0;
            for (Double d : e.getValue()) {
                sum += d;
            }
            finalPoi.setHotness(e.getValue().size());
            finalPoi.setInterest(sum / e.getValue().size());
            tempResults.add(finalPoi);
        }
        if (this.orderByInterest) {
            System.out.println("Sort by interest");
            Collections.sort(tempResults, new Comparator<POI>() {

                @Override
                public int compare(POI o1, POI o2) {
                    if (o1.getInterest() > o2.getInterest()) {
                        return -1;
                    } else if (o1.getInterest() < o2.getInterest()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });
        } else {
            System.out.println("Sort by hotness");
            Collections.sort(tempResults, new Comparator<POI>() {

                @Override
                public int compare(POI o1, POI o2) {
                    if (o1.getHotness() > o2.getHotness()) {
                        return -1;
                    } else if (o1.getHotness() < o2.getHotness()) {
                        return 1;
                    } else {        // if hotness is the same, sort by interest
                        if (o1.getInterest() > o2.getInterest()) {
                            return -1;
                        } else if (o1.getInterest() < o2.getInterest()) {
                            return 1;
                        } else {
                            return 0;
                        }
                    }
                }
            });
        }
        this.results = new POIList();
        for (POI p : tempResults) {
            this.results.addPOI(p);
        }
    }

    public boolean isOrderByInterest() {
        return orderByInterest;
    }

    public void setOrderByInterest(boolean orderByInterest) {
        this.orderByInterest = orderByInterest;
    }

}
