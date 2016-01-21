package com.basho.riakts;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;

public class DescribeTable {
	public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException, ParseException {
		// Create the Riak TS client to use to write data to
		// Update the IP and Port if needed to connect to your cluster
	    RiakClient client = RiakClient.newClient(8087, "127.0.0.1");
	    
	    //
		String queryText = "DESCRIBE WeatherStationData";
		System.out.println(queryText);
		
		// Send the query to Riak TS
		Query query = new Query.Builder(queryText).build();
		QueryResult queryResult = client.execute(query);
		
		List<Row> rows = queryResult.getRowsCopy();
		System.out.println("    Column Name    |     Type    |   Allow Null");
		System.out.println("----------------------------------------------------------------------------------");
		for (Row row : rows) {
			List<Cell> cells = row.getCellsCopy();
			String rowOut = "";
			for (Cell cell : cells) {
				if (cell != null) rowOut += Utility.getCellStringVal(cell) + "\t\t";
			}
			System.out.println(rowOut);
		}
	    client.shutdown();
	}
	
}
