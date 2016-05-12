package com.basho.riakts;

import java.net.UnknownHostException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.query.timeseries.QueryResult;

/***
 * CreateTable.java
 * @author cvitter
 * Demonstrates how to create a table in Riak TS via the Java Client API
 * For more information see the Java Client API documentation at: 
 * http://docs.basho.com/riakts/latest/developing/java/
 */
public class CreateTable {
	public static void main(String[] args) throws UnknownHostException {
		// Create the Riak TS client to use to write data to
		// Update the IP and Port if needed to connect to your cluster
	    RiakClient client = RiakClient.newClient(8087, "127.0.0.1"); 

	    // Table Create Statement for the rest of the examples
	    // See http://docs.basho.com/riakts/latest/using/creating-activating/ for
	    // more information on table creation in Riak TS
	    String queryText = "CREATE TABLE WeatherStationData " + 
	    		"( " +
	    			"device 		varchar   	not null, " +
	    			"deviceId		varchar   	not null, " +
	    			"time        	timestamp 	not null, " +
	    			"temperature 	double		not null, " +
	    			"humidity 		double, " +
	    			"pressure 		double, " +
	    			"windSpeed 		double, " +
	    			"windDirection	double, " +
	    			"PRIMARY KEY ( " +
	    			"(device, deviceId, quantum(time, 1, 'd')), " +
	    			"	device, deviceId, time " +
	    			") " +
	    		")";

	    // Create and Execute the Query object against the Riak Client
	    Query query = new Query.Builder(queryText).build();
		try {
			QueryResult queryResult = client.execute(query);
			System.out.println("Table Created Successfully");
		} catch (Exception e) {
			System.out.println("Table Creation Failed: " + e.getMessage());
		}
		
	    client.shutdown();
	}
}
