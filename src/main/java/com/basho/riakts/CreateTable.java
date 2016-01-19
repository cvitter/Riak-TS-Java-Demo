package com.basho.riakts;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.query.timeseries.QueryResult;

public class CreateTable {
	public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException {
		// Create the Riak TS client to use to write data to
	    RiakClient client = RiakClient.newClient(8087, "127.0.0.1"); 

	    // Table Create Statement for the rest of the examples
	    String queryText = "CREATE TABLE WeatherStationData " + 
	    		"( " +
	    			"device 			varchar   	not null, " +
	    			"deviceId			varchar   	not null, " +
	    			"time        		timestamp 	not null, " +
	    			"temperature 		double		not null, " +
	    			"humidity 		double, " +
	    			"pressure 		double, " +
	    			"windSpeed 		double, " +
	    			"windDirection	double, " +
	    			"PRIMARY KEY ( " +
	    			"(device, deviceId, quantum(time, 1, 'd')), " +
	    			"	device, deviceId, time " +
	    			") " +
	    		")";

	    Query query = new Query.Builder(queryText).build();
		QueryResult queryResult = client.execute(query);
		if (queryResult.getRowsCount() == 0)  System.out.println("Table Created Successfully");
		
	    client.shutdown();
	}
}
