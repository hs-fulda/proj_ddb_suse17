package fjdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;

import fdbs.CustomLogger;
import fdbs.QueryExecutor;
import parser.ParseException;
import sun.security.x509.FreshestCRLExtension;

public class FedStatement implements FedStatementInterface {
  String fsrClosed = "FedStatement resource is closed.";
  // Map of JDBC Statements
  private HashMap<Integer, Statement> statementsMap;

  private FedConnection connection;

  private boolean isClose = true;

  public FedStatement(FedConnection connection,
      HashMap<Integer, Statement> statementsMap) {
    this.connection = connection;
    this.statementsMap = statementsMap;
    isClose = false;

    // Initialize query executor to use JDBC statements
    QueryExecutor.setFedStatement(this);
    QueryExecutor.setStatementsMap(statementsMap);
  }

  public int executeUpdate(String query) throws FedException {
    //    CustomLogger.log(Level.FINE, "SQL: " + query);
    int result = -1;
    if (isClose) {
      CustomLogger.log(Level.WARNING, fsrClosed);
      throw new FedException(new Throwable(fsrClosed));
    }
    
    CustomLogger.log(Level.INFO, "Received FJDBC: " + query);
    try {
      result = QueryExecutor.executeUpdate(query);
    } catch (ParseException e) {
      CustomLogger.log(Level.WARNING, "ParseException; " + e);
      throw new FedException(new Throwable(e.getMessage()));
    }
    return result;
  }

  public FedConnection getConnection() throws FedException {
    if (isClose) {
      throw new FedException(new Throwable(fsrClosed));
    }
    return connection;
  }

  public void close() throws FedException {
    try {
      // Closes all JDBC Statements
      for (Statement statement : statementsMap.values()) {
	statement.close();
      }
      isClose = true;
    } catch (SQLException e) {
      CustomLogger.log(Level.WARNING, "FedException; " + e.getMessage());
      throw new FedException(new Throwable(e.getMessage()));
    }
  }

  /*
   * 
   * Not used yet, might/might not needs to be changed. So can't comment on it
   * at the moment.
   */

  public FedResultSet executeQuery(String sql) throws FedException {
    //    CustomLogger.log(Level.FINE, "SQL: " + sql);
    CustomLogger.log(Level.INFO, "Received FJDBC: " + sql);
    if (isClose) {
      throw new FedException(new Throwable(fsrClosed));
    }

    FedResultSet fedResultSet = null;
    List<ResultSet> resultSets = new ArrayList<ResultSet>();
    try {
      for (Statement statement : statementsMap.values()) {
	ResultSet resultSet = statement.executeQuery(sql);
	resultSets.add(resultSet);
      }
      fedResultSet = new FedResultSet(resultSets);
    } catch (SQLException e) {
      CustomLogger.log(Level.WARNING, "FedException; " + e.getMessage());
      throw new FedException(new Throwable(e.getMessage()));
    }
    return fedResultSet;
  }
}