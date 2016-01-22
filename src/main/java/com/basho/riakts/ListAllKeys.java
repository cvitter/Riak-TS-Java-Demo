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
