package gr.ntua.ece.cslab.modissense.queries.clients.containers;

import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class FriendsList {

    private final byte[] key;
    private LinkedList<UserIdStruct> friends;

    public FriendsList(UserIdStruct user) {
        key = user.getBytes();
        friends = new LinkedList<>();
    }

    /**
     *
     * @return returns the current user's key
     */
    public byte[] getKey() {
        return key;
    }

    /**
     *
     * @return returns a list with the keys of current user's friends, as they
     * are represented in our HBase table
     */
    public LinkedList<UserIdStruct> getFriends() {
        return friends;
    }

    /**
     * friend list deserializer
     *
     * @param bytes
     * @throws java.io.UnsupportedEncodingException
     */
    public void parseBytes(byte[] bytes) throws UnsupportedEncodingException {
        int index = 0;

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        this.friends = new LinkedList<>();
        int sizeOfFriendsList = buffer.getInt(index);
        index += Integer.SIZE / 8;
        UserIdStruct uid;
        for (int i = 0; i < sizeOfFriendsList; i++) {
            byte[] newFriend = new byte[this.key.length];
            for (int j = 0; j < newFriend.length; j++) {
                newFriend[j] = bytes[index + j];
            }
            index += this.key.length;
            uid = new UserIdStruct();
            uid.parseBytes(newFriend);
            friends.add(uid);
        }

    }

    /**
     * friend list serializer
     *
     * @return
     * @throws java.io.UnsupportedEncodingException
     */
    public byte[] getBytes() throws UnsupportedEncodingException {

        int totalSize = Integer.SIZE / 8;

        for (int i = 0; i < friends.size(); i++) {
            totalSize += this.key.length;
        }

        byte[] serializable = new byte[totalSize];

        ByteBuffer buffer = ByteBuffer.wrap(serializable);

        buffer.putInt(friends.size());
        for (UserIdStruct friendKey : friends) {
            buffer.put(friendKey.getBytes());
        }

        return serializable;
    }

    public byte[] getCompressedBytes() {

        byte[] serialization;
        try {
            serialization = this.getBytes();
            Deflater deflater = new Deflater();
            deflater.setLevel(4);
            deflater.setInput(serialization);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            deflater.finish();
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer);
                stream.write(buffer, 0, count);
            }

            stream.close();
            byte[] result = stream.toByteArray();
            return result;
        } catch (UnsupportedEncodingException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    public void parseCompressedBytes(byte[] array) {
        Inflater inflater = new Inflater();
        inflater.setInput(array);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = 0;
            try {
                count = inflater.inflate(buffer);
            } catch (DataFormatException e) {
                e.printStackTrace();
            }
            stream.write(buffer, 0, count);
        }
        try {
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] decompressed = stream.toByteArray();
        try {
            this.parseBytes(decompressed);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        
//        buffer.append()
        for(UserIdStruct f : this.friends) {
            buffer.append(f.toString());
            buffer.append(',');
        }
        return buffer.toString();
    }
    
    
}
