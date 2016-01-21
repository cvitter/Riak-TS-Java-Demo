# Riak TS - Java Demonstration Code
Sample Java code that demonstrates how to use the Java client to work with Riak TS (Time Series) - Riak TS is a distributed NoSQL key/value store optimized for time series data. It provides a time series database solution that is extensible and scalable. For more information on Riak TS please see the documentation homepage at: http://docs.basho.com/riakts/latest/

# What This Code Does
The code in this sample application is designed to demonstrate how to use the current Riak Java client (https://github.com/basho/riak-java-client) to interact with Riak TS and the features in the 1.1 release (http://docs.basho.com/riakts/latest/releasenotes/). 

There are eight separate class files in this initial release:

1. CreateTable.java - Creates and activates the WeatherStationData table in your Riak TS cluster. Running this class more than once will return the following error: "Failed to create table WeatherStationData: already_active".
2. DescribeTable.java - Returns the schema of the table if it has been created.
3. WriteTo.java - Writes a set of records to the WeatherStationData table.
4. ReadRows.java - Reads a range of records (based on a time range) and outputs the rows and columns to the console.
5. ReadAggregates.java - Reads a range of records (based on a time range) and outputs the total count of rows in the range and min, max, and average values of the temperature field.
6. ReadArithmetic.java - Reads a range of records (based on a time range) and outputs the results of arithmetic operations on three columns (temperature, pressure, and windSpeed)
7. ReadSingleKey.java - Reads one record using the record's primary key.
8. Utility.java - Helper class

Please submit PRs and Issues.
