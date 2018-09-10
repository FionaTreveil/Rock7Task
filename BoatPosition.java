/*
 * BoatPosition - class to hold details of a position report for a boat
 * Includes all data from the original json file. Fileds that are user are
 * boatId - Id of the boat
 * date - date/time of this position as string
 * millisecs - date/time of this position as milliseconds
 * latitude/longitude - position of the boat
 * hour - timestamp of the start of this hour
 */
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.*;

import org.json.simple.JSONObject;


public class BoatPosition {
	private Long boatId;
	private Long posId;
	private String date;
	private Long millisecs;
	private Long cog;
	private Double sog;
	private Double longitude;
	private Double latitude;
	private Long hour;

	public BoatPosition(JSONObject pos, Long boat) {
		boatId = boat;
		posId = (Long) pos.get("id");
		date = (String) pos.get("gpsAt");
		millisecs = (Long)pos.get("gpsAtMillis");
		cog = (Long)pos.get("cog");
		sog = (Double)pos.get("sogKnots");
		longitude = (Double)pos.get("longitude");
		latitude = (Double)pos.get("latitude");
		hour = dateToMilli(date.substring(0, 14) + "00:00");
	}
	public BoatPosition(ResultSet rs) {
		try {
			boatId = rs.getLong(4);
			date = rs.getString(5);
			latitude = rs.getDouble(6);
			longitude = rs.getDouble(7);
		}
	    catch(Exception e)
	    {
	        e.printStackTrace();
	    }
	}
	
	public Long getBoatId() {
		return boatId;
	}
	
	public String getDay() {
		return date.substring(0,10);
	}
	
	public Long getHour() {
		return hour;
	}
	public int getHourNumber() {
		return Integer.parseInt(date.substring(11, 13));
	}
	
	public void setDateFromMilli(Long dateMilli) {
		date = milliToDate(dateMilli);
	}
	
	public Long getLastHourInDay() {
		return dateToMilli(date.substring(0, 11) + "23:00:00") + 3600;
	}
	
	// Function to check whether 2 positions are within about 12 nm of each other
	// The full haversine formula is more complicated (time consuming) for use here
	//  so this approximation formula uses sqrt((x1-x2)squared + (y1-y2)squared))
	//  on the approximation that 0.2 degrees of both latitude and longitude is 12nm
	public Boolean isVisible(Double otherLat, Double otherLong) {
		Double latDistance = Math.abs(latitude - otherLat);
		Double longDistance = Math.abs(longitude - otherLong);
		Double distance = Math.sqrt((latDistance * latDistance) + (longDistance * longDistance));
		return (distance <= 0.2);
	}

	public static String getInsertSql() {
		return  "INSERT INTO position (idposition, time, latitude, longitude, cog, speed, boatid, timemillis, hour) VALUES (?,?,?,?,?,?,?,?,?)";
	}
	public void setSqlParams(PreparedStatement ps, Long useHour) {
		try {
		    ps.setLong(1, posId);
			ps.setString(2, date.replaceAll("-","/").replace("T"," ").replace("Z",""));
			ps.setDouble(3, latitude);
			ps.setDouble(4, longitude);
			ps.setLong(5, cog);
			ps.setDouble(6, sog);
			ps.setLong(7, boatId);
			ps.setLong(8, millisecs);
			ps.setLong(9, useHour);
        } catch (SQLException e) {
        }
	}
	public static Long dateToMilli(String date) {
		return (LocalDateTime.parse(           // Parse into an object representing a date with a time-of-day but without time zone and without offset-from-UTC.
			    date					   // Convert input string to comply with standard ISO 8601 format.
			    .replace( " " , "T" )      // Replace SPACE in the middle with a `T`.
			    .replace( "/" , "-" )      // Replace SLASH in the middle with a `-`.
			)
			.atZone(                       // Apply a time zone to provide the context needed to determine an actual moment.
			    ZoneId.of( "Europe/Oslo" ) // Specify the time zone you are certain was intended for that input.
			)                              // Returns a `ZonedDateTime` object.
			.toInstant()                  // Adjust into UTC.
			.toEpochMilli()) / 1000;       // Get the number of seconds since first moment of 1970 in UTC, 1970-01-01T00:00Z.
	}
	
	public static String milliToDate(Long milli) {
		LocalDateTime local =
		        LocalDateTime.ofInstant(Instant.ofEpochMilli(milli * 1000), ZoneId.of( "Europe/Oslo" ));
		String ss = local.toString();
		return (ss + ":00Z");
	}
}
