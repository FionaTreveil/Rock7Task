/*  
 * BoatVisible class contains details of the pairs of boats visible for each day
 *  also a record to hold the total number of boats visible by a boat for the day
 *  boatId = the id of the boat
 *  visibleBoatId - the id of the visible boat
 *  visibleDate - the day on which these boats are visible to each other
 *  visibleBoatCounter - total number of visible boats on a day
 *  compareString - internal variable used for easier comparison of 2 records 
 */
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class BoatVisible {
	Long boatId;
	Long visibleBoatId;
	String visibleDate;
	private String compareString;
	int visibleBoatCount;
	
	public BoatVisible(Long thisBoat, long otherBoat, String date, int count) {
		boatId = thisBoat;
		visibleBoatId = otherBoat;
		visibleDate = date;
		visibleBoatCount = count;
		compareString = boatId + " " + visibleBoatId + " " + visibleDate;
	}
	
	public String getVisibleDate() {
		return visibleDate;
	}
	
	public Boolean isMatch(BoatVisible match) {
		return match.compareString.equals(compareString);
	}

	public static String getInsertSql() {
		return  "INSERT INTO visible (idboat, visibleboat, visibleday, dailycount) VALUES (?, ?, ?, ?)";
	}
	public void setSqlParams(PreparedStatement ps) {
		try {
		    ps.setLong(1, boatId);
		    ps.setLong(2, visibleBoatId);
			ps.setString(3, visibleDate);
			ps.setInt(4, visibleBoatCount);
        } catch (SQLException e) {
        }
	}
}
