/*
 * Program to Process a json file containing the positions of a fleet of boats during a short race.
 * The result will be a database table that can be queried to find the average number of other
 *  vessels that will be visible by a boat on a specific day.
 * This resulting database will also provide a list of other boats that are visible on each day.
 * 
 * First the json file is parsed and the data is added to a mySql database
 * Each position report is tagged with the hour to which it relates
 * 
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class MainProg {

public static void main(String args[]){  
	try{  
		List<Boat> boats = new ArrayList<Boat>();
		
		Class.forName("com.mysql.cj.jdbc.Driver");  
		Connection con=DriverManager.getConnection(  
			"jdbc:mysql://localhost:3306/rock7?useSSL=false","root","root"); 
		//here rock7 is database name, root is both username and password  
		Statement stmt=con.createStatement();  
		stmt.executeUpdate("delete from visible");  
		stmt.executeUpdate("delete from position");  
		stmt.executeUpdate("delete from boat");  
		stmt.close();
		
		con.setAutoCommit(false);  
		readAndSaveJson("c:\\positions.json", con, boats);
		con.commit(); 
		
		processVisible(con, boats);
		con.commit();
		
		con.close();  
	}catch(Exception e){ System.out.println(e);}  
}

/*
 * Read the JSON file and populate the database with details of all of the boats and
 * their position reports
 */
public static int readAndSaveJson(String filename, Connection connection, List<Boat> boats) {
    JSONParser parser = new JSONParser();
    int count = 0;
    try {
        Object object = parser
                .parse(new FileReader(filename));
        
        //convert Object to JSONObject
        JSONObject jsonObject = (JSONObject)object;
        
        //Reading the array
        JSONArray teams = (JSONArray)jsonObject.get("teams");

        for (Object team : teams) {
        	count += 1;
        	//Reading the team and positions
        	Boat boat = new Boat((JSONObject)team);
        	
        	boats.add(boat);
        	
			PreparedStatement boatPs = connection.prepareStatement(Boat.getInsertSql());            
    		boat.setSqlParams(boatPs);  
    		boatPs.addBatch();
    		boatPs.executeBatch();
    		boatPs.close();
 
    		//Read the JSON file and put all of the positions into an array
    		JSONArray positions = (JSONArray)((JSONObject)team).get("positions");
        	
            for (Object pos : positions) {
            	BoatPosition boatPos = new BoatPosition((JSONObject)pos, boat.getId());
        		boat.addPosition(boatPos);
           	}

            //Process all of the position records.
            //Remove multiple positions within an hour and add extra dummy position
            //  records to make sure that there is a single record for each hour of the race
            BoatPosition boatPos;
            BoatPosition nextPos;
            Long lastHour;
            Long thisHour;
            int hourNumber;
            PreparedStatement posPs = connection.prepareStatement(BoatPosition.getInsertSql());            
            for (int i = 0; i < boat.countPositions(); i++) {	
        		boatPos = boat.getPosition(i);
        		nextPos = boat.getPosition(i+1);
        		
        		thisHour = boatPos.getHour();
        		lastHour = (nextPos == null) ? boatPos.getLastHourInDay() : nextPos.getHour();
        		if (nextPos != null && thisHour.longValue() != lastHour.longValue()) {
        			boatPos.setSqlParams(posPs, thisHour);  
        			posPs.addBatch();
        		}

        		hourNumber = boatPos.getHourNumber() + 1;
        		for (thisHour = boatPos.getHour()+3600; thisHour < lastHour && hourNumber < 25; thisHour += 3600, hourNumber++) {
        			// If this day is different, then set correct one into boatPos...
        			if (hourNumber == 24) {
        				boatPos.setDateFromMilli(thisHour);
        			}
            		boatPos.setSqlParams(posPs, thisHour);  
            		posPs.addBatch();
        		}
        	}
    		posPs.executeBatch();
        
    		posPs.close();
    		
        }
    }
    catch(FileNotFoundException fe) {
        fe.printStackTrace();
    }
    catch(Exception e) {
        e.printStackTrace();
    }
    return count;
}

/*
 * Process each of the boats to find all boats that could be visible on each day of the race
 */
public static void processVisible(Connection connection, List<Boat> boats) {
	try {
        PreparedStatement visPs = connection.prepareStatement(BoatVisible.getInsertSql());            
		for (Boat boatInList : boats) {
			System.out.println(boatInList.getId() + " " + boatInList.getName());  
			boatInList.processVisible(connection);	
	        for (BoatVisible vis : boatInList.getVisible()) {
	    		vis.setSqlParams(visPs);  
	    		visPs.addBatch();
	    	}
			visPs.executeBatch();
		}
        visPs.close();
	}
    catch(SQLException e)
    {
        e.printStackTrace();
    }
}

}  