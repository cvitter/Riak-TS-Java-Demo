package com.basho.riakts;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Fetch;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;

/***
 * WriteWithSql
 * @author cvitter
 * Demonstrates how to write a single row of data to a Riak TS
 * table using the SQL INSERT command.
 * 
 * For more information see the Java Client API documentation at: 
 * http://docs.basho.com/riakts/latest/developing/java/
 * 
 * Note: This example uses the WeatherStationData table created in
 * CreateTable.java and the data written in WriteTo.java.
 */
public class WriteWithSql {
	public static void main(String[] args) throws UnknownHostException, ParseException, ExecutionException, InterruptedException {
		// Create the Riak TS client to use to write data to
		// Update the IP and Port if needed to connect to your cluster
		RiakClient client = RiakClient.newClient(8087, "127.0.0.1"); 
				
		// Create timestamp string for our record
		String timeStamp = "06/06/2016 12:30:00.00";
		
		// Convert string format to epoch for TS
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SS");
		Date date = sdf.parse(timeStamp);
		long timeStampEpoch = date.getTime();;
		
	    // TS SQL INSERT - Insert a single row into TS using the SQL INSERT command
		// See http://docs.basho.com/riakts/latest/using/writingdata/#adding-data-via-sql
		// for more information about using INSERT.
		String insertQuery = 	"INSERT INTO WeatherStationData " +
								"(device, deviceId, time, temperature, humidity, pressure, windSpeed, windDirection) " +
								"VALUES " +
								"('Weather Station 0001', 'abc-xxx-001-001', " + timeStampEpoch + ", 14.5, 52.0, 20.0, 10.0, 176.0)";
		System.out.println(insertQuery);
		
		// Send the query to Riak TS to save our record
		Query query = new Query.Builder(insertQuery).build();
		QueryResult queryResult = client.execute(query);

		// Retrieve the record we just wrote to make sure its there
		final List<Cell> keyCells = 
				Arrays.asList(new Cell("Weather Station 0001"), new Cell("abc-xxx-001-001"), Cell.newTimestamp(timeStampEpoch));
		
		// Use the Fetch class to pass the table name and primary key to the client
		Fetch fetch = new Fetch.Builder("WeatherStationData", keyCells).build();
		queryResult = client.execute(fetch);
		
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