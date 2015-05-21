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

    private static final String TABLE_NAME_TEXT_REPO = "TextRepo",
            TABLE_NAME_FRIENDS = "ModisUsers",
            TABLE_NAME_FRIENDS_INFO = "UserDetailsTable";

    // fields used for input
    private UserIdStruct userId;
    private long poiId;

    // fields used to carry output results
    private int personalizedHotness;        //done
    private double personalizedInterest=0.0;    //done
    private String comment;                 //done
    private String commentUser;
    private String commentUserPicURL;
    private int numberOfFriendsComments;    //done

    // intermediate friends list
    private List<UserIdStruct> friendsList;
    private UserIdStruct maxUser;
    private Random random;

    /**
     * Empty constructor, does nothing by default.
     */
    public GetPOIClient() {
        random = new Random();
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
        this.loadFriendsInfo();
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
        for (UserIdStruct u : this.friendsList) {
            byte[] row = new byte[u.getBytes().length + poiIdToBytes.length];
            int index = 0;
            for (byte b : poiIdToBytes) {
                row[index++] = b;
            }
            for (byte b : u.getBytes()) {
                row[index++] = b;
            }
            getList.add(new Get(row));
        }
        HTable table = new HTable(HBaseConfiguration.create(), TABLE_NAME_TEXT_REPO);
        List<Result> friendsComments = new LinkedList<>();
        for (Result r : table.get(getList)) {
            if (!r.isEmpty()) {
                friendsComments.add(r);
            }
        }
        this.personalizedHotness = friendsComments.size();
        int count = 0;

        ModissenseText maxText = null;
        maxUser = null;

        for (Result r : friendsComments) {
            ModissenseText text = new ModissenseText();
            UserIdStruct currentUser = new UserIdStruct();
//            System.out.println(L);
            byte[] buffer = new byte[r.getRow().length - Long.SIZE / 8];
            for (int i = Long.SIZE / 8; i < r.getRow().length; i++) {
                buffer[i - Long.SIZE / 8] = r.getRow()[i];
            }
            currentUser.parseBytes(buffer);
            for (Map.Entry<byte[], byte[]> kv : r.getFamilyMap("t".getBytes()).entrySet()) {
                text.parseBytes(kv.getValue());
                this.personalizedInterest += text.getScore();
                count++;
                if (maxText == null || maxText.getScore() < text.getScore()) {
                    maxText = text;
                    maxUser = currentUser;
                } else if (maxText.getScore()==text.getScore() && this.random.nextBoolean()) {
                    maxText = text;
                    maxUser = currentUser;
                }
            }
        }
        if(count!=0) {
            this.personalizedInterest /= count;
        } else {
            this.personalizedInterest = 0;
        }
        this.numberOfFriendsComments = count;
        if(maxText!=null)
            this.comment = maxText.getText();

//        System.out.println(maxUser);
    }

    private void loadFriendsInfo() throws IOException {
        if(this.maxUser==null)
            return;
        HTable table = new HTable(HBaseConfiguration.create(), TABLE_NAME_FRIENDS_INFO);
        Get get = new Get(this.maxUser.getBytes());
        Result rs = table.get(get);
        byte[] result = rs.getValue("cf".getBytes(), "f".getBytes());
        ByteBuffer buffer = ByteBuffer.wrap(result);

        int length = buffer.getInt();
        byte[] userNameBuffer = new byte[length];
        buffer.get(userNameBuffer);
        this.commentUser = new String(userNameBuffer,"UTF-8");
        length = buffer.getInt();
        byte[] userPictureURL = new byte[length];
        buffer.get(userPictureURL);
        this.commentUserPicURL = new String(userPictureURL,"UTF-8");
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
}
