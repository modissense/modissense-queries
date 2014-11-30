package gr.ntua.ece.cslab.modissense.queries.clients.simple;

import gr.ntua.ece.cslab.modissense.queries.containers.POI;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * This query returns the static info for a POI; the name, coordinates,
 * keywords, etc. are considered as not user-specified info and these are
 * returned. Programmatically, this is translated to an SQL query to the
 * PostgreSQL Server.
 *
 * @author Giannis Giannakopoulos
 */
public class GetPOIStaticInfo {

    private long poiId;

    private Connection connection = null;
    private Properties properties = null;

    private POI result = null;

    public GetPOIStaticInfo() {
    }

    public GetPOIStaticInfo(long poiId) {
        this.poiId = poiId;
    }

    public void executeQuery() throws SQLException, IOException {
        this.openTable();
        Statement statement = this.connection.createStatement();
        ResultSet rs = statement.executeQuery(
                "SELECT *, ST_X(geo) AS x, ST_Y(geo) AS y FROM " + this.properties.getProperty("postgres.table.pois")
                + " WHERE poi_id=" + this.poiId);
        while (rs.next()) {
            if (this.result == null) {
                this.result = new POI();
            }
            this.result.setId(rs.getLong("poi_id"));
            if (rs.getString("keywords") != null) {
                this.result.setKeywords(new HashSet(Arrays.asList(rs.getString("keywords").split(","))));
            }
            this.result.setName(rs.getString("name"));
            this.result.setX(rs.getDouble("x"));
            this.result.setY(rs.getDouble("y"));
            this.result.setHotness(rs.getDouble("hotness"));
            this.result.setInterest(rs.getDouble("interest"));
        }
        rs.close();
        statement.close();

    }

    public POI getResult() {
        return this.result;
    }

    private void openTable() throws SQLException, IOException {
        if (this.properties == null) {
            InputStream resources = this.getClass().getClassLoader().getResourceAsStream("modissense-clients.properties");
            if (resources == null) {
                System.err.println("I need modissense-clients.properties in my classpath");
                return;
            }
            properties = new Properties();
            properties.load(resources);
        }
        if (this.connection == null) {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(GetUserStructIds.class.getName()).log(Level.SEVERE, null, ex);
            }
            String url = "jdbc:postgresql://["
                    + properties.getProperty("postgres.server") + "]/"
                    + properties.getProperty("postgres.db") + "?"
                    + "user=" + properties.getProperty("postgres.user") + "&"
                    + "password=" + properties.getProperty("postgres.pass");

            this.connection = DriverManager.getConnection(url);
        }
    }

    public static void main(String[] args) throws IOException, SQLException {
        if (args.length < 1) {
            System.out.println("I need poid id as argument");
            System.exit(1);
        }
        int poiId = new Integer(args[0]);

        GetPOIStaticInfo query = new GetPOIStaticInfo(poiId);
        query.executeQuery();
        System.out.println("RESULT:\t" + query.getResult());
    }

}
