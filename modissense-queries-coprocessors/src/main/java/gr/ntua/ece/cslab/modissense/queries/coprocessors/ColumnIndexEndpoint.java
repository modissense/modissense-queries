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
import java.util.Map;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Giannis Giannakopoulos
 */
public class ColumnIndexEndpoint extends BaseEndpointCoprocessor implements ColumnIndexProtocol {

    @Override
    public byte[] getPOIs(UserCheckinsQueryArguments argument) throws IOException {
        System.out.println("Logging info:\t"+argument.getUserIds());
        POIList result = new POIList();
        HRegion region =  ((RegionCoprocessorEnvironment)getEnvironment()).getRegion();
        UserIdStruct firstUser = new UserIdStruct();
        if(region.getStartKey().length!=0)
            firstUser.parseBytes(region.getStartKey());
        else
            firstUser = null;
        UserIdStruct lastUser = new UserIdStruct();
        if(region.getEndKey().length!=0)
            lastUser.parseBytes(region.getEndKey());
        else
            lastUser = null;
        for(UserIdStruct u : argument.getUserIds()) {
            if((firstUser == null || u.compareTo(firstUser)>-1) && (lastUser == null || u.compareTo(lastUser)<0)) {
                Get get = new Get(u.getBytes());
                get.setFilter(new ColumnRangeFilter(Bytes.toBytes(argument.getStartTimestamp()), true, Bytes.toBytes(argument.getEndTimestamp()), true));
                Result res = region.get(get);
                POIList userResults = new POIList();
                if(res != null && !res.isEmpty()) {
                    for(Map.Entry<byte[], byte[]> e : res.getFamilyMap(DefaultTableDesign.COLUMN_FAMILY.getBytes()).entrySet()) {
                        POI p = new POI();
                        p.parseBytes(e.getValue());
                        userResults.addPOI(p);
                    }
                }
                userResults.clearList(argument.getKeywords());
                userResults.clearList(argument.getxFrom(), argument.getyFrom(), argument.getxTo(), argument.getyTo());
                for(POI p : userResults.getPOIs())
                    result.addPOI(p);
            }
        }
        return result.getCompressedBytes();
    }
    
}
