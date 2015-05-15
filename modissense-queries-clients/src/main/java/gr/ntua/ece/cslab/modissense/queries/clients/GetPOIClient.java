/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.ntua.ece.cslab.modissense.queries.clients;

import gr.ntua.ece.cslab.modissense.queries.clients.containers.FriendsList;
import gr.ntua.ece.cslab.modissense.queries.containers.ModissenseText;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Giannis Giannakopoulos
 */
public class GetPOIClient {

    private final String 
            TABLE_NAME_TEXT_REPO = "TextRepo",
            TABLE_NAME_FRIENDS = "ModisUsers";

    // fields used for input
    private UserIdStruct userId;
    private long poiId;

    // fields used to carry output results
    private int personalizedHotness;
    private double personalizedInterest;
    private String comment;
    private String commentUser;
    private String commentUserPicURL;
    private int numberOfFriendsComments;

    // intermediate friends list
    private List<UserIdStruct> friendsList;

    /**
     * Empty constructor, does nothing by default.
     */
    public GetPOIClient() {
    }

    /**
     * Constructor that initializes the
     *
     * @param userId
     * @param poiId
     */
    public GetPOIClient(UserIdStruct userId, long poiId) {
        this.userId = userId;
        this.poiId = poiId;
    }
    
    
    
    // public interface
    public void executeQuery() throws IOException {
        this.loadFriends();
        this.parseFriendComments();
    }

    // util methods
    private void loadFriends() throws IOException {
        HTable table = new HTable(HBaseConfiguration.create(), TABLE_NAME_FRIENDS);
        UserIdStruct qId = new UserIdStruct(this.userId.getC(), this.userId.getId());
        Get get = new Get(qId.getBytes());
        Result r = table.get(get);
        for (Map.Entry<byte[], byte[]> kv : r.getFamilyMap("ids".getBytes()).entrySet()) {
            FriendsList list = new FriendsList(qId);
            list.parseCompressedBytes(kv.getValue());
            this.friendsList = list.getFriends();
        }
        System.out.println("Friends loaded:"+this.friendsList);
    }

    /**
     * Returns the number of comments (globally, for all users) for a given poi.
     *
     * @param poiId
     * @return the number of comments made
     * @throws java.io.IOException
     */
    private void parseFriendComments() throws IOException {
        List<Get> getList = new LinkedList<>();
        byte[] poiIdToBytes = Bytes.toBytes(this.poiId);
        for(UserIdStruct u:this.friendsList) {
            byte[] row = new byte[u.getBytes().length+poiIdToBytes.length];
            int index=0;
            for(byte b:poiIdToBytes)
                row[index++]=b;
            for(byte b:u.getBytes())
                row[index++]=b;
            getList.add(new Get(row));
        }
        HTable table = new HTable(HBaseConfiguration.create(), TABLE_NAME_TEXT_REPO);
        Result[] results =table.get(getList);
        this.numberOfFriendsComments=results.length;
        
        List<ModissenseText> list = new LinkedList<>();
        for(Result r: results) {
            ModissenseText text = new ModissenseText();
        }
    }
    
    // Getters and setters
    public void setUserId(UserIdStruct userId) {
        this.userId = userId;
    }

    public void setPoiId(long poiId) {
        this.poiId = poiId;
    }

    
    // == 
    public int getPersonalizedHotness() {
        return personalizedHotness;
    }

    public double getPersonalizedInterest() {
        return personalizedInterest;
    }

    public String getComment() {
        return comment;
    }

    public String getCommentUser() {
        return commentUser;
    }

    public String getCommentUserPicURL() {
        return commentUserPicURL;
    }

    public int getNumberOfFriendsComments() {
        return numberOfFriendsComments;
    }

    public static void main(String[] args) throws IOException {
        GetPOIClient client = new GetPOIClient(new UserIdStruct('F', 100008415518168L), 11534L);
        
        client.executeQuery();
        System.out.println(client.getNumberOfFriendsComments());

//        }
    }

}
