package com.basho.riakts;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;

public class ReadRows {
	public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException, ParseException {
		// Create the Riak TS client to use to write data to
		// Update the IP and Port if needed to connect to your cluster
		RiakClient client = RiakClient.newClient(8087, "127.0.0.1"); 
			    
		// Start and end date to search on
		String startDateStr = "19/01/2016 12:30:00.00";
		String endDateStr = "19/01/2016 12:45:00.00";
		
		// Convert string formats to epoch for TS
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SS");
		Date date = sdf.parse(startDateStr);
		long startDate = date.getTime();
		date = sdf.parse(endDateStr);
		long endDate = date.getTime();
		
	    // TS SQL Query - explicitly selects all of the columns in the table
		// for a given time range, note that you must include a time range as well
		// as the device and deviceId in this query as they are a part of the
		// table's primary key. See http://docs.basho.com/riakts/latest/using/querying/
		// for more information about querying TS.
		String queryText = "select device, deviceId, time, temperature, humidity, " +
				"pressure, windSpeed, windDirection from WeatherStationData " +
				"where time > " + startDate + " and time < " + endDate + " and " +
				"device = 'Weather Station 0001' and deviceId = 'abc-xxx-001-001'";
		System.out.println(queryText);
		
		// Send the query to Riak TS
		Query query = new Query.Builder(queryText).build();
		QueryResult queryResult = client.execute(query);
		
		// Output the number of rows returned to the console
		System.out.println("Total Rows Returned: " + queryResult.getRowsCount() );
		
		// Iterate over the returned rows and print them out to the console
		List<Row> rows = queryResult.getRowsCopy();
		for (Row row : rows) {
			List<Cell> cells = row.getCellsCopy();
			String rowOut = "";
			for (Cell cell : cells) {
				rowOut += Utility.getCellStringVal(cell) + " | ";
			}
			System.out.println(rowOut);
		}
		
	    client.shutdown();
	}

}
