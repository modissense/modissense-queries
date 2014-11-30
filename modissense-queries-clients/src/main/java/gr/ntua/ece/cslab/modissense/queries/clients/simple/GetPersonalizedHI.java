package gr.ntua.ece.cslab.modissense.queries.clients.simple;

import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;

/**
 *
 * This query returns the Hotness and the Interest for a specified POI in a
 * personalized way. The scores change for different users, even when the POI 
 * remains the same.
 * @author Giannis Giannakopoulos
 */
public class GetPersonalizedHI {
    
    private UserIdStruct userId;
    private int poiId;

    public GetPersonalizedHI() {
    }
}
