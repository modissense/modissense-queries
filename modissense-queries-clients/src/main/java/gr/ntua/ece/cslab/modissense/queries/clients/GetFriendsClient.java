/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.ntua.ece.cslab.modissense.queries.clients;

import gr.ntua.ece.cslab.modissense.queries.clients.containers.FriendsList;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

/**
 *
 * @author giannis
 */
public class GetFriendsClient {
    
        private static final String TABLE_NAME_FRIENDS = "ModisUsers";


    private List<UserIdStruct> friendsList;
    private UserIdStruct userId;
    
    
    public GetFriendsClient(UserIdStruct userId) {
        this.userId = userId;
        this.friendsList = new LinkedList<>();
    }
    
    
    public void executeQuery() throws IOException {
        this.loadFriends();
    }

    public List<UserIdStruct> getFriendsList() {
        return friendsList;
    }
    
    
    // util methods
    private void loadFriends() throws IOException {
        HTable table = new HTable(HBaseConfiguration.create(), TABLE_NAME_FRIENDS);
        UserIdStruct qId = new UserIdStruct(this.userId.getC(), this.userId.getId());
        System.out.println("Userid is:\t"+this.userId);
        Get get = new Get(this.userId.getBytes());
        Result r = table.get(get);
        if(r==null || r.isEmpty() || r.getFamilyMap("ids".getBytes())==null) {
            return;
        }
        for (Map.Entry<byte[], byte[]> kv : r.getFamilyMap("ids".getBytes()).entrySet()) {
            FriendsList list = new FriendsList(qId);
            list.parseCompressedBytes(kv.getValue());
            this.friendsList = list.getFriends();
        }
    }

    
    
    
    
    
}
