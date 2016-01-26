package com.basho.riakts;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.ListKeys;
import com.basho.riak.client.api.commands.timeseries.ListKeys.Builder;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;

/***
 * ListAllKeys
 * @author cvitter
 * Demonstrates how to use the ListKeys builder to return all of the primary keys in a table
 * HOWEVER listing all of the keys is an expensive operation that shouldn't be run
 * against a production cluster for performance reasons.
 * 
 * For more information see the Java Client API documentation at: 
 * http://docs.basho.com/riakts/latest/developing/java/
 * 
 * Note: This example uses the WeatherStationData table created in
 * CreateTable.java and the data written in WriteTo.hava. The code will not fail
 * if that code hasn't been successfully executed against your Riak TS cluster first 
 * however no date will be returned in the QueryResult object.
 */
public class ListAllKeys {
	public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException, ParseException {
		// Create the Riak TS client to use to write data to
		// Update the IP and Port if needed to connect to your cluster
		RiakClient client = RiakClient.newClient(8087, "127.0.0.1"); 

		// WARNING - Listing Keys is an expensive operation and shouldn't
		// be done in production
		
		// ListKeys Builder for the "WeatherStationData" table
		// Returns the primary key of each row in the QueryResult set
		Builder listKeys = new ListKeys.Builder("WeatherStationData");
		QueryResult queryResult = client.execute( listKeys.build() );
			
		// Get Iterator from QueryResult
		Iterator<Row> rows = queryResult.iterator();
		while (rows.hasNext()) {
			Row row = (Row) rows.next();
			
			Iterator<Cell> cells = row.iterator();
			String rowOut = "";
			while (cells.hasNext()) {
				Cell cell = (Cell) cells.next();
				rowOut += Utility.getCellStringVal( cell ) + " ";
			}

			System.out.println(rowOut);
		}
		System.out.println("Total Keys: " + queryResult.getRowsCount());

		client.shutdown();
	}
}
