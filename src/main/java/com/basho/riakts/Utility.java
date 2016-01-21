package com.basho.riakts;

import java.util.Date;

import com.basho.riak.client.core.query.timeseries.Cell;

public class Utility {

	/***
	 * Converts timeseries.cell values to strings for output
	 * @param cell
	 * @return
	 */
	public static String getCellStringVal(Cell cell) {
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
			// Output both the epoch and human readable data format
			return cell.getTimestamp() + " | " + out.toString();
		}
		else if (cell.hasVarcharValue()) {
			return cell.getVarcharAsUTF8String();
		}
		else {
			return null;
		}	
	}
	
}
