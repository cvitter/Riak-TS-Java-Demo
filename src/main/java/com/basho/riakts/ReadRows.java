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
		
		// Start and end date to search on
		String startDateStr = "19/01/2016 12:30:00.00";
		String endDateStr = "19/01/2016 12:45:00.00";
		
		// Convert string formats to epoch for TS
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SS");
		Date date = sdf.parse(startDateStr);
		long startDate = date.getTime();
		date = sdf.parse(endDateStr);
		long endDate = date.getTime();
		
	    // Riak Client with supplied IP and Port 
	    RiakClient client = RiakClient.newClient(8087, "127.0.0.1"); 
	    
	    // TS SQL Query
		String queryText = "select device, deviceId, time, temperature, humidity, " +
				"pressure, windSpeed, windDirection from WeatherStationData " +
				"where time > " + startDate + " and time < " + endDate + " and " +
				"device = 'Weather Station 0001' and deviceId = 'abc-xxx-001-001'";
		System.out.println(queryText);
		
		// Send the query to Riak TS
		Query query = new Query.Builder(queryText).build();
		QueryResult queryResult = client.execute(query);
		System.out.println("Total Rows Returned: " + queryResult.getRowsCount() );
		
		// Iterate of the return rows and print them out to the console
		List<Row> rows = queryResult.getRowsCopy();
		for (Row row : rows) {
			List<Cell> cells = row.getCellsCopy();
			String rowOut = "";
			for (Cell cell : cells) {
				rowOut += getCellStringVal(cell) + " | ";
			}
			System.out.println(rowOut);
		}
		
	    client.shutdown();
	}
	
	
	// Convert cell values to string for output
	private static String getCellStringVal(Cell cell) {
		if (cell.hasBoolean()) {
			return Boolean.toString(cell.getBoolean());
		}
		else if (cell.hasDouble()) {
			return String.valueOf(cell.getDouble());
		}
		else if (cell.hasLong()) {
			return String.valueOf(cell.getLong());
		}
		else if (cell.hasTimestamp()) {
			Date out = new Date(cell.getTimestamp()); 
			return out.toString();
		}
		else if (cell.hasVarcharValue()) {
			return cell.getVarcharAsUTF8String();
		}
		else {
			return null;
		}	
	}
	
}
