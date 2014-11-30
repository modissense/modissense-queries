package gr.ntua.ece.cslab.modissense.queries.containers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Class used to hold all the data
 *
 * @author Giannis Giannakopoulos
 *
 */
public class POIList implements Serializable, Compressible {

    private static final int COMPRESSION_LEVEL = 4;
    private List<POI> poiList;

    public POIList() {
        this.poiList = new LinkedList<>();
    }

    public void addPOI(POI poi) {
        this.poiList.add(poi);
    }

    public List<POI> getPOIs() {
        return this.poiList;
    }

    public void chopLowScores(int highRanksToKeep) {
        if (highRanksToKeep >= this.poiList.size()) {
            return;
        }

        List<Double> list = new LinkedList<>();
        for (POI p : this.poiList) {
            list.add(p.getScore());
        }
        Collections.sort(list);
        double pivot = list.get(highRanksToKeep - 1);

//		double pivot = this.getKthBiggerNumber(highRanksToKeep);
        List<POI> newPoiList = new LinkedList<>();
        for (POI p : this.poiList) {
            if (p.getScore() >= pivot) {
                newPoiList.add(p);
            }
        }
        this.poiList = newPoiList;
    }

    public void clearList(double xFrom, double yFrom, double xTo, double yTo) {
        List<POI> list = new LinkedList<POI>();
        for (POI p : this.poiList) {
            double x = p.getX();
            double y = p.getY();
            if (x >= xFrom && x <= xTo && y >= yFrom && y <= yTo) {
                list.add(p);
            }
        }
        this.poiList = list;
    }

    public void clearList(List<String> keywords) {
        if (keywords != null && !keywords.isEmpty()) {
            List<POI> list = new LinkedList<>();
            for (POI p : this.poiList) {
                for (String s : keywords) {
                    if (p.getKeywords().contains(s) || p.getName().toLowerCase().contains(s.toLowerCase())) {
                        list.add(p);
                        break;
                    }
                }
            }
            this.poiList = list;
        }
    }

    public void clearList(long startTimestamp, long endTimestamp) {
        List<POI> list = new LinkedList<>();
        for (POI p : this.poiList) {
            if (p.getTimestamp() >= startTimestamp && p.getTimestamp() <= endTimestamp) {
                list.add(p);
            }
        }
        this.poiList = list;
    }

    @Override
    public void parseBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        this.poiList = new LinkedList<>();
        int sizeOfList = buffer.getInt();
        for (int i = 0; i < sizeOfList; i++) {
            int byteSize = buffer.getInt();
            byte[] poiSerial = new byte[byteSize];
            buffer.get(poiSerial, 0, byteSize);
            POI poi = new POI();
            poi.parseBytes(poiSerial);
            this.poiList.add(poi);
        }
    }

    @Override
    public byte[] getBytes() {
        // sort pois based on timestamp before serializing them
        Collections.sort(this.poiList, new Comparator<POI>() {

            @Override
            public int compare(POI o1, POI o2) {
                if (o1.getTimestamp() > o2.getTimestamp()) {
                    return 1;
                } else if (o1.getTimestamp() < o2.getTimestamp()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
        int numberOfBytes = Integer.SIZE / 8;		// number of POIs stored
        for (POI p : this.poiList) {
            numberOfBytes += Integer.SIZE / 8;
            numberOfBytes += p.getBytes().length;
        }

        byte[] bytes = new byte[numberOfBytes];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        buffer.putInt(this.poiList.size());
        for (POI p : this.poiList) {
            byte[] bytesPoi = p.getBytes();
            buffer.putInt(bytesPoi.length);
            buffer.put(bytesPoi);
        }
        return bytes;
    }

    @Override
    public byte[] getCompressedBytes() {
        byte[] serialization = this.getBytes();

        Deflater deflater = new Deflater();
        deflater.setLevel(COMPRESSION_LEVEL);
        deflater.setInput(serialization);

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        deflater.finish();
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            stream.write(buffer, 0, count);
        }
        try {
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] result = stream.toByteArray();
        return result;
    }

    @Override
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

        this.parseBytes(decompressed);

    }

    @Override
    public String toString() {
        return this.poiList.toString();
    }

    public static void main(String[] args) throws IOException, DataFormatException {
//        POI a, b;
//        Set<String> keywords1 = new HashSet<String>(), keywords2 = new HashSet<String>();
//        keywords1.add("wine");
//        keywords1.add("bar");
//        keywords2.add("italian");
//        keywords2.add("food");
//
//        a = new POI();
//        a.setName("Foobara+Foomara");
//        a.setX(50.0);
//        a.setY(51.0);
//        a.setScore(12.1);
//        a.setKeywords(keywords1);
//        a.setTimestamp(Long.MAX_VALUE);
//
//        b = new POI();
//        b.setName("HelloWorld");
//        b.setX(51.0);
//        b.setY(12.2);
//        b.setScore(9.1);
//        b.setKeywords(keywords2);
//        b.setTimestamp(Long.MIN_VALUE);
//
//        POIList list = new POIList();
//        list.addPOI(a);
//        list.addPOI(b);
//        for (int i = 0; i < 100; i++) {
//            list.addPOI(b);
//        }
//
//        System.out.println("List consists of:\t" + list.getPOIs().size());
//
//        POIList newList = new POIList();
//        newList.parseCompressedBytes(list.getCompressedBytes());
//        System.out.println("New list consists of:\t" + newList.getPOIs().size());
        HTable table = new HTable(HBaseConfiguration.create(), "poilist-index");
        Get get = new Get(Bytes.toBytes(1l));
        Result res = table.get(get);
        if (!res.isEmpty()) {
            byte[] buffer = res.getValue("cf".getBytes(), "".getBytes());
            System.out.println("Compressed length:\t" + buffer.length);
            POIList list = new POIList();
            list.parseCompressedBytes(buffer);
            System.out.println("Uncompressed length:\t" + list.getBytes().length);
        }

        table.close();
    }

}
