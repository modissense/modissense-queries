/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.ntua.ece.cslab.modissense.queries.coprocessors;

import gr.ntua.ece.cslab.modissense.queries.containers.POI;
import gr.ntua.ece.cslab.modissense.queries.containers.POIList;
import gr.ntua.ece.cslab.modissense.queries.containers.UserCheckinsQueryArguments;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 *
 * @author giannis
 */
public class POIListEndpoint  extends BaseEndpointCoprocessor implements POIListProtocol {

    @Override
    public byte[] getPOIs(UserCheckinsQueryArguments argument) throws IOException {
        POIList result = new POIList();
        HRegion region =  ((RegionCoprocessorEnvironment)this.getEnvironment()).getRegion();
        UserIdStruct firstUser = new UserIdStruct();
        firstUser.parseBytes(region.getStartKey());
        UserIdStruct lastUser = new UserIdStruct();
        lastUser.parseBytes(region.getEndKey());
        for(UserIdStruct u : argument.getUserIds()) {
            if(u.compareTo(firstUser)>-1 && u.compareTo(lastUser)<0) {
                Get get = new Get(u.getBytes());
                Result res = region.get(get);
                if(res != null && !res.isEmpty()) {
                    POIList list = new POIList();
                    list.parseBytes(res.getValue(DefaultTableDesign.COLUMN_FAMILY.getBytes(), DefaultTableDesign.QUALIFIER.getBytes()));
                    list.clearList(argument.getKeywords());
                    list.clearList(argument.getxFrom(), argument.getyFrom(), argument.getxTo(), argument.getyTo());
                    list.clearList(argument.getStartTimestamp(), argument.getEndTimestamp());
                    list.chopLowScores(10);
                    for(POI p : list.getPOIs())
                        result.addPOI(p);
                }
                System.out.println("Size:\t"+result.getBytes().length);
            }
        }
        return result.getCompressedBytes();
    }
    
}
