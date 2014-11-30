package gr.ntua.ece.cslab.modissense.queries.containers;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.util.Bytes;

public class POI implements Serializable {

    private Long id;
    private Long timestamp;
    private String name;
    private Double x, y;
    private Set<String> keywords;
    private Double score;
    
    private double hotness;
    private double interest;

    public POI() {
        this.id = Long.MAX_VALUE;
        this.timestamp = new Date().getTime();
        this.name = new String();
        this.x = Double.MAX_VALUE;
        this.y = Double.MAX_VALUE;
        this.keywords = new HashSet<>();
        this.score = new Double(Long.MAX_VALUE);
    }

    public double getHotness() {
        return hotness;
    }

    public void setHotness(double hotness) {
        this.hotness = hotness;
    }

    public double getInterest() {
        return interest;
    }

    public void setInterest(double interest) {
        this.interest = interest;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public Double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public Set<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(Set<String> keywords) {
        this.keywords = keywords;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long poiId) {
        this.id = poiId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + Objects.hashCode(this.id);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final POI other = (POI) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "[" + this.id + "] " 
                + this.name + " @ " + "(" + this.x + ", " + this.y 
                + ") tagged with " + this.keywords + " with score " + this.score +" at "
                +new Date(this.timestamp) + "hotness: "+this.hotness+", interest: "+this.interest;
    }

    @Override
    public void parseBytes(byte[] bytes) {
        int index = 0;

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        this.id = buffer.getLong(index);
        index += Long.SIZE / 8;
        
        this.timestamp = buffer.getLong(index);
        index += Long.SIZE / 8;

        int bytesOnName = buffer.getInt(index);
        index += Integer.SIZE / 8;

        this.name = new String(bytes, index, bytesOnName);
        index += bytesOnName;

        this.x = buffer.getDouble(index);
        index += Double.SIZE / 8;

        this.y = buffer.getDouble(index);
        index += Double.SIZE / 8;

        this.score = buffer.getDouble(index);
        index += Double.SIZE / 8;

        int keywordsCount = buffer.getInt(index);
        index += Integer.SIZE / 8;

        this.keywords = new HashSet<>();

        for (int i = 0; i < keywordsCount; i++) {
            int keywordSize = buffer.getInt(index);
            index += Integer.SIZE / 8;
            String keyword = new String(bytes, index, keywordSize);
            index += keyword.length();
            this.keywords.add(keyword);
        }

    }

    @Override
    public byte[] getBytes() {
        try {
            int totalSize = Long.SIZE / 8 // poi id
                    + Long.SIZE / 8 // timestamp id
                    + Integer.SIZE / 8 + this.name.getBytes("UTF-8").length // bytes needed to write string size + string
                    + Double.SIZE / 8 // x coord
                    + Double.SIZE / 8 // y coord
                    + Double.SIZE / 8 // score
                    + Integer.SIZE / 8;					// size of set
            for (String s : keywords) // string size + actual string for all keywords
            {
                totalSize += Integer.SIZE / 8 + s.getBytes("UTF-8").length;
            }
            byte[] serializable = new byte[totalSize];

            ByteBuffer buffer = ByteBuffer.wrap(serializable);
            buffer.put(Bytes.toBytes(this.id));
            buffer.put(Bytes.toBytes(this.timestamp));
            buffer.put(Bytes.toBytes(this.name.getBytes("UTF-8").length));
            buffer.put(this.name.getBytes("UTF-8"));
            buffer.put(Bytes.toBytes(this.x));
            buffer.put(Bytes.toBytes(this.y));
            buffer.put(Bytes.toBytes(this.score));
            buffer.put(Bytes.toBytes(this.keywords.size()));
            for (String s : this.keywords) {
                buffer.put(Bytes.toBytes(s.getBytes("UTF-8").length));
                buffer.put(s.getBytes("UTF-8"));
            }
            return serializable;
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(POI.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println("Starting main execution");
        POI poi = new POI();

        poi.setId(1l);
        poi.setName("Φραουλόκηπος");
        poi.setX(10.2);
        poi.setY(10.3);
        poi.setScore(55.0);
        Set<String> keyws = new HashSet<>();
        keyws.add("καφές");
        keyws.add("wine");
        poi.setKeywords(keyws);
        System.out.println("Old poi:\t" + poi);

        byte[] serial = poi.getBytes();

        System.out.println("Bytes serialized:\t" + serial.length);

        POI newPoi = new POI();
        newPoi.parseBytes(serial);
        System.out.println("New poi:\t" + newPoi);
    }

}
