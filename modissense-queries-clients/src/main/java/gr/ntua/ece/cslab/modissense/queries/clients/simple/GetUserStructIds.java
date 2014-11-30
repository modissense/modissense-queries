package gr.ntua.ece.cslab.modissense.queries.clients.simple;

import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This query returns the UserStructId objects related to a modissense user.
 *
 * @author Giannis Giannakopoulos
 */
public class GetUserStructIds {

    private int userId;

    private Connection connection = null;
    private Properties properties = null;

    private List<UserIdStruct> result;

    public GetUserStructIds() {
    }

    public GetUserStructIds(int userId) {
        this.userId = userId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void executeQuery() throws SQLException, IOException {
        this.openTable(); 
        Statement statement = this.connection.createStatement();
        ResultSet rs = statement.executeQuery(
                "SELECT * FROM " + this.properties.getProperty("postgres.table.sn_list")
                + " WHERE user_id=" + this.userId);
        
        this.result = new LinkedList<>();
        while (rs.next()) {
            switch(rs.getString("sn_name").toLowerCase()) {
                case "twitter" : 
                    this.result.add(new UserIdStruct('t', rs.getLong("user_id")));
                    break;
                case "facebook" : 
                    this.result.add(new UserIdStruct('F', rs.getLong("user_id")));
                    break;
                case "foursquare" : 
                    this.result.add(new UserIdStruct('f', rs.getLong("user_id")));
                    break;
                default: break;
            }
        }
        rs.close();
        statement.close();
    }

    public List<UserIdStruct> getResult() {
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
            System.out.println("I need user id as argument");
            System.exit(1);
        }
        int userid = new Integer(args[0]);

        GetUserStructIds query = new GetUserStructIds(userid);
        query.executeQuery();
        System.out.println("Modissense user " + userid + " is also indexed as " + query.getResult());
    }
}
