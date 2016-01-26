package com.basho.riakts;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Delete;
import com.basho.riak.client.core.query.timeseries.Cell;

/***
 * DeleteSingleKey
 * @author cvitter
 * Demonstrates the how to delete a single row from Riak TS using the primary key.
 * 
 * For more information see the Java Client API documentation at: 
 * http://docs.basho.com/riakts/latest/developing/java/
 * 
 * Note: This example uses the WeatherStationData table created in
 * CreateTable.java and the data written in WriteTo.java however attempting to delete a key
 * that doesn't exist will not throw an error
 */
public class DeleteSingleKey {
	public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException, ParseException {
		// Create the Riak TS client to use to write data to
		// Update the IP and Port if needed to connect to your cluster
		RiakClient client = RiakClient.newClient(8087, "127.0.0.1"); 

		// A records primary key is a three part key containing: Family, Series, Quantum
		// To delete a single records we create a List<Cell> with a cell for the family, series and quantum 
		// See http://docs.basho.com/riakts/latest/using/querying/#Single-Key-Fetch for more information
		final List<Cell> keyCells = 
				Arrays.asList(new Cell("Weather Station 0001"), new Cell("abc-xxx-001-001"), Cell.newTimestamp(1453225380001L));
		
		// Use the Delete class to pass the table name and primary key to the client to be deleted
		// NOTE: The operation will report as successful as long as no error is thrown, 
		// if a key doesn't exist the client will still return success
		Delete delete = new Delete.Builder("WeatherStationData", keyCells).build();
		try {
			client.execute(delete);	
			System.out.println("Delete opertion successful");
		}
		catch (Exception e) {
			System.out.println("Delete opertion failed: " + e.getMessage());
		}
	    client.shutdown();
	}
}
