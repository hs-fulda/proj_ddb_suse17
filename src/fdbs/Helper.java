package fdbs;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.*;

import application.ApplicationConstants;
import fdbs.*;
import parser.ParseException;
import java.util.*;
public class Helper {
	
	public static InputStream convertToParsableQuery(String query) {
		InputStream parsableQuery = new ByteArrayInputStream(query.getBytes());
		return parsableQuery;
	}
	
	/**
	 * @parameter query
	 * @author Jahan
	 *
	 */
	public static void PrintResult(String query) throws SQLException {
			Connection conn = null;
			Statement stmt = null;
			conn = DriverManager.getConnection(ConnectionConstants.CONNECTION_3_URL, ApplicationConstants.USERNAME, ApplicationConstants.PASSWORD);
		    query = "SELECT * FROM PASSAGIER";
		    
		    try {
		        stmt = conn.createStatement();
		        ResultSet rs = stmt.executeQuery(query);
		        ResultSetMetaData rsmd = rs.getMetaData();		        
		        int columnCount = rsmd.getColumnCount();		       
		        // Initialize the metadata list
		        List<String> x = new ArrayList<String>();			
				
		        for (int i = 1; i <= columnCount; i++ ) {
		          String name = rsmd.getColumnName(i);
		          // Push to array
		          x.add(name);
		          // Print Columns
		          System.out.printf("%-10s", name);
		        }
		        System.out.print("\n----------");
		        System.out.print("-------------------------------");
		        System.out.print("\n");
		        while(rs.next()){		            
		            for (int i = 0; i < columnCount; i++) {
		            	//Display values
						System.out.printf("%-10s", rs.getString(x.get(i)));						
					}
		            System.out.print("\n");
		         }
		    } catch (SQLException e ) {
		        System.out.println(e);
		    }
		}
	//TODO delete the main method
	public static void main(String[] args) throws ParseException {
		try {
			// Pass query
			PrintResult("s");
		} catch (SQLException e) {
			System.out.println(e);
		}
	}
	
}
