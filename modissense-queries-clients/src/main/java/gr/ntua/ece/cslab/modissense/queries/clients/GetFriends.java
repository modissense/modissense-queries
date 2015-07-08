/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.ntua.ece.cslab.modissense.queries.clients;

import gr.ntua.ece.cslab.modissense.queries.clients.containers.FriendsList;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.IOException;
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
public class GetFriends {
    
        private static final String TABLE_NAME_FRIENDS = "ModisUsers";


    private List<UserIdStruct> friendsList;
    private UserIdStruct userId;
    
    
    public GetFriends(UserIdStruct userId) {
        this.userId = userId;
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
        Get get = new Get(qId.getBytes());
        Result r = table.get(get);
        for (Map.Entry<byte[], byte[]> kv : r.getFamilyMap("ids".getBytes()).entrySet()) {
            FriendsList list = new FriendsList(qId);
            list.parseCompressedBytes(kv.getValue());
            this.friendsList = list.getFriends();
        }
    }

    
    
    
    
    
}
