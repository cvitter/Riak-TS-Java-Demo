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

public class ReadAggregates {
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
	    
	    // TS SQL Query - Aggregate count on rows matching WHERE clause
		String queryText = "select COUNT(*) from WeatherStationData " +
				"where time > " + startDate + " and time < " + endDate + " and " +
				"device = 'Weather Station 0001' and deviceId = 'abc-xxx-001-001'";
		System.out.println(queryText);
		
		// Send the query to Riak TS
		Query query = new Query.Builder(queryText).build();
		QueryResult queryResult = client.execute(query);
		
		// Iterate of the return rows and print them out to the console
		List<Row> rows = queryResult.getRowsCopy();
		for (Row row : rows) {
			List<Cell> cells = row.getCellsCopy();
			String rowOut = "";
			for (Cell cell : cells) {
				rowOut += getCellStringVal(cell);
			}
			System.out.println("COUNT(*) Returns: " + rowOut);
		}
		
		// Get the min, avg, and max temperature readings for the same selection of records
		queryText = "select MIN(temperature), AVG(temperature), MAX(temperature) from WeatherStationData " +
				"where time > " + startDate + " and time < " + endDate + " and " +
				"device = 'Weather Station 0001' and deviceId = 'abc-xxx-001-001'";
		System.out.println(queryText);
		// Send the query to Riak TS
		query = new Query.Builder(queryText).build();
		queryResult = client.execute(query);
		
		// Iterate of the return rows and print them out to the console
		rows = null;
		rows = queryResult.getRowsCopy();
		for (Row row : rows) {
			List<Cell> cells = row.getCellsCopy();
			int cellNo = 0;
			for (Cell cell : cells) {
				if (cellNo==0) {
					System.out.println("Miniumum Temp: " + getCellStringVal(cell));
				}
				else if (cellNo==1) {
					System.out.println("Average Temp: " + getCellStringVal(cell));
				}
				else if (cellNo==2) {
					System.out.println("Maximum Temp: " + getCellStringVal(cell));
				}
				cellNo++;
			}
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
