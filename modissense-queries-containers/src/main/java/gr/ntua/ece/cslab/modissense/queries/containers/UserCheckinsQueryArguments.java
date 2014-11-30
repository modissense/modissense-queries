package gr.ntua.ece.cslab.modissense.queries.containers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.io.Writable;

/**
 * Plain Bean used to pass query arguments inside the coprocessor. 
 * @author Giannis Giannakopoulos
 */
public class UserCheckinsQueryArguments implements Writable {
    
    private List<UserIdStruct> userIds;
    private Long startTimestamp, endTimestamp;
    private List<String> keywords;
    private double xFrom, yFrom, xTo, yTo;

    public UserCheckinsQueryArguments() {
        this.userIds = new LinkedList<>();
        this.keywords = new LinkedList<>();
        this.startTimestamp = 0l;
        this.endTimestamp = Long.MAX_VALUE;
    } 
    
    public List<UserIdStruct> getUserIds() {
        return userIds;
    }

    public void setUserIds(List<UserIdStruct> userIds) {
        this.userIds = userIds;
    }

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(Long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(Long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }

    public double getxFrom() {
        return xFrom;
    }

    public void setxFrom(double xFrom) {
        this.xFrom = xFrom;
    }

    public double getyFrom() {
        return yFrom;
    }

    public void setyFrom(double yFrom) {
        this.yFrom = yFrom;
    }

    public double getxTo() {
        return xTo;
    }

    public void setxTo(double xTo) {
        this.xTo = xTo;
    }

    public double getyTo() {
        return yTo;
    }

    public void setyTo(double yTo) {
        this.yTo = yTo;
    }
    
    
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.userIds.size());
        for(UserIdStruct u : this.userIds){
            out.writeChar(u.getC());
            out.writeLong(u.getId());
        }
        out.writeInt(this.keywords.size());
        for(String s : keywords)
            out.writeUTF(s);
        out.writeLong(this.startTimestamp);
        out.writeLong(this.endTimestamp);
        out.writeDouble(this.xFrom);
        out.writeDouble(this.yFrom);
        out.writeDouble(this.xTo);
        out.writeDouble(this.yTo);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userIds = new LinkedList<>();
        this.keywords = new LinkedList<>();
        this.startTimestamp = 0l;
        this.endTimestamp = Long.MAX_VALUE;

        int usersCount = in.readInt();
        for(int i=0;i<usersCount;i++) 
            this.userIds.add(new UserIdStruct(in.readChar(), in.readLong()));
        int keywsCount = in.readInt();
        for(int i=0;i<keywsCount;i++)
            this.keywords.add(in.readUTF());
        
        this.startTimestamp = in.readLong();
        this.endTimestamp = in.readLong();
        this.xFrom = in.readDouble();
        this.yFrom = in.readDouble();
        this.xTo = in.readDouble();
        this.yTo = in.readDouble();
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException {
        RandomAccessFile file = new RandomAccessFile("temp.txt", "rw");
        LinkedList<String> keywords = new LinkedList<>();
        keywords.add("bar");keywords.add("foo");
        LinkedList<UserIdStruct> users = new  LinkedList<>();
        users.add(new UserIdStruct('F', 1l)); 
        users.add(new UserIdStruct('F',2l)); 
        
        UserCheckinsQueryArguments argum = new UserCheckinsQueryArguments();
        argum.setEndTimestamp(Long.MAX_VALUE);
        argum.setStartTimestamp(Long.MIN_VALUE);
        argum.setKeywords(keywords);
        argum.setUserIds(users);
        argum.setxFrom(0.0);
        argum.setyFrom(0.0);
        argum.setxTo(100.0);
        argum.setyTo(100.0);
        
        argum.write(file);
        file.close();
        
        file = new RandomAccessFile("temp.txt", "r");
        UserCheckinsQueryArguments newArgs = new UserCheckinsQueryArguments();
        
        newArgs.readFields(file);
        file.close();
    }
    
    @Override
    public String toString() {
        StringBuilder builder =new StringBuilder();
        builder.append(this.userIds);
        builder.append(this.keywords);
        builder.append(this.startTimestamp);
        builder.append(this.endTimestamp);
        builder.append(this.xFrom);
        builder.append(this.xTo);
        builder.append(this.yFrom);
        builder.append(this.yTo);
        return builder.toString();
    }
    
}
