/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.ntua.ece.cslab.modissense.queries.coprocessors;

import gr.ntua.ece.cslab.modissense.queries.containers.UserCheckinsQueryArguments;
import java.io.IOException;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 *
 * @author Giannis Giannakopoulos
 */
public interface POIListProtocol extends CoprocessorProtocol {
    
    public byte[] getPOIs(UserCheckinsQueryArguments argument) throws IOException;
}
