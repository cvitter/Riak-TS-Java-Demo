package com.basho.riakts;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;

/***
 * DescribeTable 
 * @author cvitter
 * Demonstrates how to use the DESCRIBE command in Riak TS  via the Java Client API 
 * to view a table's schmea or simply verify a table has been created
 * For more information see the Java Client API documentation at: 
 * http://docs.basho.com/riakts/latest/developing/java/
 * 
 * Note: This example uses the WeatherStationData table created in
 * CreateTable.java and will fail if that code hasn't been successfully
 * executed against your Riak TS cluster first.
 */
public class DescribeTable {
	public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException, ParseException {
		// Create the Riak TS client to use to write data to
		// Update the IP and Port if needed to connect to your cluster
	    RiakClient client = RiakClient.newClient(8087, "127.0.0.1");
	    
	    // Verify the creation of the table using DESCRIBE
	    // http://docs.basho.com/riakts/latest/using/creating-activating/#Verification-via-Client-Library
		String queryText = "DESCRIBE WeatherStationData";
		System.out.println(queryText);
		
		// Send the query to Riak TS
		Query query = new Query.Builder(queryText).build();
		QueryResult queryResult = client.execute(query);
		
		// Note: There are two ways to view information about a table's columns using
		// the results of the running DESCRIBE demonstrated below:

		// Example 1. Iterate over the QueryResult object -
		//  - Rows in the result set represents a column in the table
		//	- Cells represent the following in order: column name, data type, allows null, 
		//		partition key position, local key position
		
		// Write the output of the DESCRIBE command
		System.out.println("    Column Name    |     Type    |   Allow Null");
		System.out.println("----------------------------------------------------------------------------------");

		Iterator<Row> rows = queryResult.iterator();
		while (rows.hasNext()) {
			Row row = (Row) rows.next();
			
			Iterator<Cell> cells = row.iterator();
			String rowOut = "";
			while (cells.hasNext()) {
				Cell cell = (Cell) cells.next();
				if ( cell != null) rowOut += Utility.getCellStringVal( cell ) + "\t\t";
			}
			System.out.println(rowOut);
		}
		System.out.println("");
		
		// Example 2. Read .getColumnDescriptionsCopy from the QueryResult object
		// which returns a ColumnDescription object for each column that you can
		// iterate over for to retrieve the name and type of the column
		
		// getColumnDescriptionsCopy - a list of column names and their data types
		List<ColumnDescription> cd = queryResult.getColumnDescriptionsCopy();
		Iterator<ColumnDescription> cds = cd.iterator();
		while (cds.hasNext()) {
			ColumnDescription desc = cds.next();
			System.out.println("Column Name: " + desc.getName() + "   - Data Type: " + desc.getType());
		}
		
	    client.shutdown();
	}
	
}
