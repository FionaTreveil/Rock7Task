/*
 * Boat class holds details of a boat
 * name - name of the boat
 * id - if of the boat
 * visibleBoats - list of all visible boats on each day
 *
 */
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;

public class Boat {
	private String name;
	private Long id;
	private List<BoatPosition> positions = new ArrayList<BoatPosition>();
	private List<BoatVisible> visibleBoats = new ArrayList<BoatVisible>();

	public Boat(JSONObject boatJson){
		name = (String) boatJson.get("name");
    	id = (Long) boatJson.get("marker");
	}
	
	public Long getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	
	public List<BoatPosition> getPositions() {
		return positions;
	}
	
	public void addPosition(BoatPosition position) {
		positions.add(0, position);
	}
	public int countPositions() {
		return positions.size();
	}
	public BoatPosition getPosition(int index) {
		if (index < positions.size()) {
			return positions.get(index);
		} else {
			return null;
		}
	}
	
	public List<BoatVisible> getVisible() {
		return visibleBoats;
	}
	
	public static String getInsertSql() {
		return  "INSERT INTO boat (idboat, name) VALUES (?, ?)";
	}
	public void setSqlParams(PreparedStatement ps) {
		try {
		    ps.setLong(1, id);
			ps.setString(2, name);
        } catch (SQLException e) {
        }
	}
	
	/*
	 * Find all of the possible visible boats for each hour.
	 * Store the boats visible per day with the total count for the day
	 */
	public void processVisible(Connection connection) {
		try {
			Double latitude, longitude;
			BoatPosition otherPosition;
			Boolean found;
			String thisDay = "";
			int dailyVisibleCount = 0;
			
			BoatVisible visible;
			Statement stmt=connection.createStatement();  
			//This query will find all other boat positions at the same hour where the maximum distance from
			// this boat is 0.2 degrees (about 12 miles).  The longitudinal distance is less accurate.
			ResultSet rs=stmt.executeQuery("select distinct boatpos.time, boatpos.latitude, boatpos.longitude," +
						"otherboat.boatid, otherboat.time, otherboat.latitude, otherboat.longitude, boatpos.hour " +
						"from position boatpos " + 
						"join position otherboat " +
						"on otherboat.boatid <> " + id.toString() +
						" and otherboat.hour = boatpos.hour " +
						"where boatpos.boatid = " + id.toString() +
						" and otherboat.latitude > (boatpos.latitude - 0.2) and otherboat.latitude < (boatpos.latitude + 0.2) " +
						"and otherboat.longitude > (boatpos.longitude -0.2) and otherboat.longitude < (boatpos.longitude + 0.2) " +
						"order by boatpos.time, otherboat.boatid, otherboat.timemillis");  
			while(rs.next())  {
				latitude = rs.getDouble(2);
				longitude = rs.getDouble(3);
				otherPosition = new BoatPosition(rs);
				visible = new BoatVisible(getId(), otherPosition.getBoatId(), otherPosition.getDay(), 0);  
				
				if (!visible.getVisibleDate().equals(thisDay)) {
					if ((! thisDay.equals("") && dailyVisibleCount > 0)) {
						visibleBoats.add(new BoatVisible(getId(), 0, thisDay, dailyVisibleCount));
					}
					thisDay = visible.getVisibleDate();
					dailyVisibleCount = 0;
				}

				found = false;
				for (BoatVisible foundVisible : visibleBoats) {
					if (foundVisible.isMatch(visible)) {
						found = true;
						break;
					}
				}
				//If this match is not already found, then count it and add to the list
				if (!found) {
					//Check whether the boat found is likely to be visible
					//This check may not be necessary as the longitude and latitude distance
					// filtering from the database may be sufficient for this approximation
					if (otherPosition.isVisible(latitude, longitude)) {
						visibleBoats.add(visible);
						dailyVisibleCount += 1;
					}
				}
			}
			// Add the boat visible count for the last day being processed
			if ((! thisDay.equals("") && dailyVisibleCount > 0)) {
				visibleBoats.add(new BoatVisible(getId(), 0, thisDay, dailyVisibleCount));
			}
		}
		catch(Exception e){ 
			System.out.println(e);}  	
		}
	}

