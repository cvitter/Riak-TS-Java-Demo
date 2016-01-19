package com.basho.riakts;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ExecutionException;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.query.timeseries.*;
import java.util.*;

public class WriteTo {

	public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException, ParseException {
		// Create the Riak TS client to use to write data to
	    RiakClient client = RiakClient.newClient(8087, "127.0.0.1"); 
	    
	    // Set up the data values with defaults
	    double temp = 10.0;
	    double humidity = 50.0;
	    double pressure = 29.92;
	    double windSpeed = 5.0;
	    double windDirection = 180.0;
	    
	    // Create the starting date and convert it to epoch for TS
	    String startDateStr = "19/01/2016 11:30:00.00";
	    // Convert string formats to epoch for TS
	 	SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SS");
	 	Date date = sdf.parse(startDateStr);
	 	long startDate = date.getTime();
	    
	    for (int i = 0; i < 1001; i++) {
	    	// Change the reading values over time to make the data interesting
	    	if (i % 10 == 0) temp += 0.1;
			if (i % 25 == 0) humidity += 0.25;
			if (i % 25 == 0) pressure -= 0.15;
			if (i % 10 == 0) windSpeed += 0.1;
			
			// Create the row to write to Riak TS
		    List<Row> rows = Arrays.asList(
		      new Row(
		        new Cell("Weather Station 0001"), 
		        new Cell("abc-xxx-001-001"), 
		        Cell.newTimestamp(startDate), 
		        new Cell(temp),
		        new Cell(humidity),
		        new Cell(pressure),
		        new Cell(windSpeed),
		        new Cell(windDirection)
		      )
		    ); 

		    // Create the store command and execute against Riak TS
		    Store storeCmd = new Store.Builder("WeatherStationData").withRows(rows).build(); 
		    client.execute(storeCmd); 
		    System.out.println("Line written: " + i);
		    // Add 10 seconds to startDate for the next record
		    startDate += 10000; 
	    }
	    client.shutdown();
	}

}
