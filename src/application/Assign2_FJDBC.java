package application;
/* @author  Aleksandr Anfilov,	MSc GSD, fdai5146
 */

import fjdbc.FedConnection;
import fjdbc.FedException;
import fjdbc.FedPseudoDriver;
import fjdbc.FedStatement;

import java.sql.*;
import java.util.Random;

public class Assign2_FJDBC {
  private static final String USER = "VDBSA04";
  private static final String PASS = "VDBSA04";
  private static final int TUPLES = 1000;
  private static final int NUMBER_OF_TABLES = 1; // from R1K to R1000K
  private static final String TABLENAME = "PASSENGER";
  private static int tuplesCount = TUPLES;
  private static Boolean verbose = false;
  //  static String conn_type = "wired";
  private static String conn_type = "wifi";
  private static final long dayMillis = 24 * 60 * 60 * 1000;
  private static long startTime, stopTime = 0;
  private static final String[] NAME = { "Bailleu", "Bayer", "Bostanci", "Burhenne", "Bush", "Collins", "Dehler",
		  "Englert",
		  "Fessi",
		  "Gabriel", "Gannouni", "Gueclue", "Haas", "Hahn", "Harasty", "Heinz", "Helmke", "Hennecke", "Hesse",
		  "Hohmeyer", "Horn", "Jagger", "John", "Kaas", "Klomann", "Knabe", "Kouada", "Ksaier", "Lamps",
		  "Lennox", "Lied", "Mahr", "Maiwald", "Meatloaf", "Mercury", "Meuer", "Militz", "Miosga", "Mueller",
		  "Nannini", "Nguentcheng", "Niedeggen", "Paulheim", "Pei", "Ramazotti", "Reinel", "Richards",
		  "Ruppel", "Sanama", "Saseno", "Schaefer", "Schmitt", "Scholz", "Sprenger", "Sting", "Stock",
		  "Tepel", "Tobert", "Turner", "Wald" };
  private static final String[] VORNAME = { "Alexandre", "Andre", "Anreas", "Annie", "Annika", "Aymen", "Bastian",
		  "Benedikt",
		  "Bjoern", "Bob", "Christian", "Daniel", "Donald", "Eberhard", "Elton", "Erdem", "Eros", "Florian",
		  "Franz", "Freddie", "Gaspard", "Gerhard", "Gianna", "Guildo", "Hichem", "Holger", "Hueseyin",
		  "Ilkem", "Jan", "Juergen", "Kai", "Kate", "Keith", "Lukas", "Marcel", "Mario", "Markus", "Martin",
		  "Matthias", "Melanie", "Michael", "Mick", "Nejib", "Nico", "null", "Pascal", "Patricia", "Patrik",
		  "Peter", "Phil", "Rene", "Rudi", "Sarah", "Sascha", "Sebastian", "Simon", "Sinan", "Stefan", "Susi",
		  "Tao" };
  private static final String[] LAND = { "CHN", "CMR", "D", "F", "GB", "I", "ID", "TUN", "USA" };
  /*	jDBC*/
  private static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
  private static final String DB_URL = "jdbc:oracle:thin:@mtsthelens.informatik.hs-fulda.de:1521:oralv9a";
  private static Connection conn = null;
  private static Statement stmt = null;
  private static PreparedStatement pstmt = null;
  private static ResultSet rs = null;
  /*	*/
  private static String query, tableName;

  private static FedConnection fedConnection = null;
  private static FedStatement fedStatement = null;

  public static void main(String[] args) throws SQLException, FedException {
    System.out.println("Type of the connection: " + conn_type);

    myJDBC();

    /*	FJBDC	*/
    int count = 0;
    String sqlInsert, sqlValues, name, vname, land;
    tuplesCount = NUMBER_OF_TABLES;
    System.out.println("Starting FJDBC."); 
    try {
      fedConnection = (new FedPseudoDriver()).getConnection(USER, PASS);
      fedConnection.setAutoCommit(false);
      fedStatement = fedConnection.getStatement();
    } catch (FedException fedException) {
      System.out.println(fedException.getMessage());
      System.out.flush();
    }
	
    //fjRunQuery("ALTER SESSION SET nls_language = english");

    /*	BATCH INSERT */
    for (int i = 1; i <= NUMBER_OF_TABLES; i++) {
      tableName = TABLENAME + tuplesCount / TUPLES + "K";
      sqlInsert = "INSERT INTO " + tableName + " VALUES (";
      startTime = System.currentTimeMillis();
      for (int pnr = 1; pnr <= tuplesCount; pnr++) {
	Random random = new Random();
	name = NAME[random.nextInt(NAME.length)];
	vname = VORNAME[random.nextInt(VORNAME.length)];
	land = LAND[random.nextInt(LAND.length)];
	sqlValues = pnr + "," + " '" + name + "'," + " '" + vname + "'," + " '" + land + "')";
	/* 	EXECUTE THE QUERY*/
	count += fjRunQuery(sqlInsert + sqlValues);
      }//end for tuples

      stopTime = System.currentTimeMillis() - startTime;
      System.out.println(count + " rows inserted in " + stopTime + " ms.");

      fjRunQuery("INSERT INTO Results VALUES ("
		      + "'" + tableName + "',"
		      + stopTime + ", "
		      + "'" + conn_type + "',"
		      + "'fed ', SYSDATE)");

      try {
	fedConnection.commit();
      } catch (FedException fedException) {
	fedException.printStackTrace();
	System.err.println(fedException.getMessage());
	System.out.flush();
	System.err.flush();
      }

      tuplesCount = tuplesCount * 10;
    }//end for tables

    /* PROGRAM ENDS */
    try {
      if (fedConnection != null) {
	fedConnection.close();
      }
    } catch (FedException fedException) {
      fedException.printStackTrace();
      System.err.println(fedException.getMessage());
      System.out.flush();
      System.err.flush();
    }
  }

  private static int fjRunQuery(String statement) {
    int count = 0;
    try {
      count = fedStatement.executeUpdate(statement);
    } catch (FedException fe) {
      System.err.println(fe.getMessage());
      System.err.println(query);
    }
    return count;
  }

  private static int myJDBC() throws SQLException {
    conn = jGetConnection();
    if (conn == null) {
      System.out.println("EXIT: no connection is available.");
      return 0;
    } else {
      try {
	stmt = conn.createStatement();
      } catch (SQLException e) {
	e.getMessage();
	e.getSQLState();
      }
      query = "ALTER SESSION SET nls_language = english";
      jRunQuery(query);
    /* INSERT TUPLES */
      for (int i = 1; i <= NUMBER_OF_TABLES; i++) {
	tableName = TABLENAME + tuplesCount / TUPLES + "K";

	query = "DROP TABLE " + tableName;
	jRunQuery(query);

	query = "CREATE TABLE  " + tableName
			+ " (PNR integer, NAME varchar(40), VORNAME varchar(40), LAND varchar(3), "
			+ " constraint " + tableName + "_NAME_NN check (NAME is not null), "
			+ " constraint " + tableName + "_PK primary key (PNR))";
	jRunQuery(query);

	jInsertTuples(tableName, tuplesCount);
	System.out.println("************************************************************");
	/*
	System.out.println("RESULTS FOR " + tableName);
	query = "SELECT count(*) cnt_all, COUNT(DISTINCT(NAME)) cnt_name, "
			+ "COUNT(DISTINCT(VORNAME)) cnt_vname, "
			+ "COUNT(DISTINCT(LAND)) cnt_land FROM " + tableName;
	*/
	jPrintQueryResults(" SELECT table_name, time_taken, TEST_DATE FROM RESULTS"
			+ " WHERE table_name LIKE 'PASSEN%' AND TEST_DATE > TRUNC(SYSDATE)"
			+ " ORDER BY table_name ASC");

	tuplesCount = tuplesCount * 10;
      }// end for

      jCloseConnection();
      return 1;
    }
  }

  private static void jRunQuery(String query) {
    try {
      stmt.executeUpdate(query);
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      System.err.println(query);
    }
  }

  private static void jInsertTuples(String tableName, int tuples) {
    System.out.println("Inserting " + tuples + " tuples into the table " + tableName);

    String sql = "INSERT INTO " + tableName + " VALUES (?,?,?,?)";
    try {
      pstmt = conn.prepareStatement(sql);
      startTime = System.currentTimeMillis();
      for (int pnr = 1; pnr <= tuples; pnr++) {
	Random random = new Random();
	pstmt.setInt(1, pnr);
	pstmt.setString(2, NAME[random.nextInt(NAME.length)]);
	pstmt.setString(3, VORNAME[random.nextInt(VORNAME.length)]);
	pstmt.setString(4, LAND[random.nextInt(LAND.length)]);
	pstmt.addBatch();
	// Doing batch size of 1000, because of the network aspects
	// and to avoid "out of memory error"
	if (pnr % 1000 == 0) {
	  pstmt.executeBatch();
	  pstmt.clearBatch();
	  System.out.print(".");
	}
      }// end for

      stopTime = System.currentTimeMillis() - startTime;

    } catch (SQLException sqle) {
      System.out.println("\t SQLState:" + sqle.getSQLState());
      int errorCode = -1;
      System.out.println("\t ErrorCode:" + errorCode);
      System.out.println("\t message:" + sqle.getMessage());
    } finally {
      // Only commit after all updates and inserts have been successful
      try {
	System.out.print("Attempting to commit...");
	conn.commit();
	System.out.println("done.");

	// Document execution time
	System.out.println("\nTime required to insert " + tuplesCount
			+ " tuples: " + stopTime + "ms ");
	sql = "INSERT INTO Results(table_name, time_taken, conn_type, layer)"
			+ " VALUES (?,?,?,?)";
	pstmt = conn.prepareStatement(sql);
	pstmt.setString(1, tableName);
	pstmt.setLong(2, stopTime);
	pstmt.setString(3, conn_type);
	pstmt.setString(4, "jdbc");
	pstmt.execute();
	conn.commit();


      } catch (SQLException er) {
	System.out.println("commit() failed.");
	er.printStackTrace();
	try {
	  System.out.println("Attempting to rollback...");
	  conn.rollback();
	} catch (SQLException ex) {
	  System.out.println("rollback() failed.");
	  ex.printStackTrace();
	}
      }
    }// finally
  }

  private static void jPrintQueryResults(String query) throws SQLException {
    final int TAB_WIDTH = 24;
    final String FORMAT = "%" + TAB_WIDTH + "s";
    try {
      rs = stmt.executeQuery(query);
      java.sql.ResultSetMetaData rsmd;
      rsmd = rs.getMetaData();
      int columnCount = rsmd.getColumnCount();
      for (int j = 1; j <= columnCount; j++) {
	System.out.format(FORMAT, rsmd.getColumnName(j));
      }
      System.out.println();
      while (rs.next()) {
	for (int j = 1; j <= columnCount; j++) {
	  System.out.format(FORMAT, rs.getString(j));
	}
	System.out.println();
      }
      System.out.println();
      for (int j = 1; j <= TAB_WIDTH * 10; j++) {
	System.out.print("=");
      }
      System.out.println();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private static Connection jGetConnection() {
    System.out.print("Connecting to the database...");
    try {
      Class.forName(JDBC_DRIVER);
      conn = DriverManager.getConnection(DB_URL, USER, PASS);
      conn.setAutoCommit(false);
      //      conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      System.out.println("connected.");
      return conn;
    } catch (ClassNotFoundException | SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  private static void jCloseConnection() {
    try {
      if (rs != null) {
	rs.close();
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    try {
      if (stmt != null) {
	stmt.close();
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    try {
      if (conn != null) {
	conn.close();
	System.out.println("The connection has been closed.");
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }
}
