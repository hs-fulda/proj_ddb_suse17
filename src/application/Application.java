package application;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.sun.javafx.binding.StringFormatter;

import fjdbc.FedConnection;
import fjdbc.FedException;
import fjdbc.FedPseudoDriver;
import fjdbc.FedResultSet;
import fjdbc.FedStatement;
import parser.ParseException;

public class Application {
  private static long startTime;
  private static long duration;

  public static void main(String[] args) throws ParseException, FedException {
    // Selects files and stores scripts in a list
    File selectedFile = FileUtility.getFile();
    List<String> scriptsFromSelectedFile = FileUtility.getScriptsFromFile(selectedFile);
    FedConnection fedConnection = null;
    try {

      // Gets connection based on Username and Password
      fedConnection = new FedPseudoDriver().getConnection(ApplicationConstants.USERNAME, ApplicationConstants.PASSWORD);
      fedConnection.setAutoCommit(false);

      FedStatement fedStatement = fedConnection.getStatement();

      // Just a formatting to display result
      OutputFormatter.printAstericks();
      System.out.println("Executing script file \'" + selectedFile.getAbsolutePath() + "\' ...");

      // Time starts
      startTime = System.currentTimeMillis();

      // All scripts taken from file provided run here one by one
      int totalOperations = 0;
      for (String currentScript : scriptsFromSelectedFile) {
	// If the query is DDL or DML, executeUpdate should be called
	// from FJDBC
	if (isDDLOrDMLScript(currentScript.toUpperCase()))
	  fedStatement.executeUpdate(currentScript);
	else if (isCommit(currentScript.toUpperCase()))
	  fedConnection.commit();
	else if (isRollback(currentScript.toUpperCase()))
	  fedConnection.rollback();
	else {
	  FedResultSet resultSet = fedStatement.executeQuery(currentScript);
	  printResult(resultSet);
	}
	totalOperations++;
      }

      System.out.println(totalOperations + " operations");

      // Prints Time Taken
      System.out.println(getTimeTaken());
      OutputFormatter.printAstericks();
    } catch (FedException e) {
      System.out.println(e);
    }
    // @author: Anfilov. Close all JDBC connections
    finally {
      fedConnection.close();
    }

  }

  private static void printResult(FedResultSet resultSet) throws FedException {
    List<String> columnNames = new ArrayList<String>();
    List<String> columnTypes = new ArrayList<String>();
    List<String> records = new ArrayList<String>();

    int numberOfColumns = resultSet.getColumnCount();
    
    int counter = 1;
    String column = null;
    while (counter <= numberOfColumns) {
      String columnName = resultSet.getColumnName(counter);
      columnNames.add(String.format("%-12s", columnName));
      column = resultSet.getColumnType(counter);
      columnTypes.add(column);

      // String columnType = getColumnTypeStr(resultSet.getColumnType(counter));
      // columnNames.add(String.format("%-10s", columnName + " (" + columnType +
      // ")"));
      // String columnValue = "";
      // if (columnType.equals("INTEGER"))
      // columnValue = resultSet.getInt(counter) + "";
      // else if (columnType.equals("VARCHAR"))
      // columnValue = resultSet.getString(counter);
      // columnValues.add(columnValue);
      counter++;
    }
    
     while (resultSet.next()) {
        StringBuilder record = new StringBuilder();
        counter = 0;
        while (counter < numberOfColumns) {          	
        	String columnType = columnTypes.get(counter);  	
        	String columnValue = "";
        	if (columnType.equals("INTEGER") || columnType.equals("NUMBER"))
        	  columnValue = resultSet.getInt(counter+1) + "";
        	else if (columnType.equals("VARCHAR"))
        	  columnValue = resultSet.getString(counter+1);
        	record.append(String.format("%-12s", columnValue));
        	counter++;
        }
     // @author: Jahan. Check for duplication. Only insert unique data  
     if (!records.contains(record.toString())) {
          records.add(record.toString());          
      } else {
          continue;
      }
       
    }
    
    String columnNamesStr = "";
    for (String string : columnNames) {
      columnNamesStr += string;
    }
   
    System.out.println(columnNamesStr);
    System.out.print("-------------------------------------------------------");
    System.out.print("\n");
    for (String string : records) {
      System.out.println(string);
    }

  }

  private static String getColumnTypeStr(int columnTypeInt) {
    String columnType = "";
    switch (columnTypeInt) {
    case 1:
      columnType = "INTEGER";
      break;
    case 2:
      columnType = "VARCHAR";
      break;
    default:
    }
    return columnType;
  }

  private static boolean isRollback(String script) {
    return script.startsWith("ROLLBACK");
  }

  private static boolean isCommit(String script) {
    return script.startsWith("COMMIT");
  }

  /**
   * @param script
   * @return
   */
  private static boolean isDDLOrDMLScript(String script) {
    return script.startsWith("CREATE") || script.startsWith("DROP") || script.startsWith("INSERT")
	|| script.startsWith("DELETE") || script.startsWith("UPDATE") || script.startsWith("ALTER")
	|| script.startsWith("SET");
  }

  private static String getTimeTaken() {
    duration = System.currentTimeMillis() - startTime;
    Date timeTaken = new Date(duration);
    String timeTakenStr = String.format("Time Taken : %2d Min : %2d Sec : %3d Millis", timeTaken.getMinutes(),
	timeTaken.getSeconds(), (timeTaken.getTime() % 1000));
    return timeTakenStr;
  }

}
