/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.ntua.ece.cslab.modissense.queries.clients;

/**
 *
 * @author Giannis Giannakopoulos
 */
public class GetPOIClient {

    public GetPOIClient() {
    }
    
    
    /**
     * Returns the number of comments (globally, for all users), for a poi based 
     * 
     * @param poiId
     * @return 
     */
    public int getNumberOfComments(int poiId) {
        return 1;
    }
    
}
