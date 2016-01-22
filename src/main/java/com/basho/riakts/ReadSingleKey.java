package com.basho.riakts;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Fetch;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;

public class ReadSingleKey {
	
	public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException, ParseException {
		// Create the Riak TS client to use to write data to
		// Update the IP and Port if needed to connect to your cluster
		RiakClient client = RiakClient.newClient(8087, "127.0.0.1"); 

		// A records primary key is a three part key containing: Family, Series, Quantum
		// To fetch a single records we create a List<Cell> with a cell for the family, series and quantum 
		// See http://docs.basho.com/riakts/latest/using/querying/#Single-Key-Fetch for more information
		final List<Cell> keyCells = 
				Arrays.asList(new Cell("Weather Station 0001"), new Cell("abc-xxx-001-001"), Cell.newTimestamp(1453225380000L));
		
		// Use the Fetch class to pass the table name and primary key to the client
		Fetch fetch = new Fetch.Builder("WeatherStationData", keyCells).build();
		QueryResult queryResult = client.execute(fetch);
		
		// Get Iterator from QueryResult
		Iterator<Row> rows = queryResult.iterator();
		while (rows.hasNext()) {
			Row row = (Row) rows.next();
			
			Iterator<Cell> cells = row.iterator();
			String rowOut = "";
			while (cells.hasNext()) {
				Cell cell = (Cell) cells.next();
				rowOut += Utility.getCellStringVal( cell ) + " | ";
			}

			System.out.println(rowOut);
		}
	    client.shutdown();
	}

}
