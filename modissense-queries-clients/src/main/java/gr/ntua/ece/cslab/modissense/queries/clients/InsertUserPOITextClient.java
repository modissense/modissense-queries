package gr.ntua.ece.cslab.modissense.queries.clients;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import gr.ntua.ece.cslab.modissense.queries.containers.ModissenseText;
import gr.ntua.ece.cslab.modissense.queries.containers.UserPoiStruct;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertUserPOITextClient extends AbstractQueryClient{
	
	private UserPoiStruct userPOIkey;
	private ModissenseText text;
	
	public UserPoiStruct getUserPOIkey() {
		return userPOIkey;
	}

	public void setUserPOIkey(UserPoiStruct userPOIkey) {
		this.userPOIkey = userPOIkey;
	}

	public ModissenseText getText() {
		return text;
	}

	public void setText(ModissenseText text) {
		this.text = text;
	}

	@Override
	public void executeQuery() {
		 try {
	            Put put = new Put(this.userPOIkey.getBytes());
	            put.add("t".getBytes(), Bytes.toBytes(this.text.getTimestamp()), this.text.getBytes());
	            this.table.put(put);
	            
	        } catch (IOException ex) {
	            Logger.getLogger(InsertPOIVisitClient.class.getName()).log(Level.SEVERE, null, ex);
	        }
	}

}
