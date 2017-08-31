package fdbs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import fjdbc.FedException;
import fjdbc.FedStatement;
import parser.GepardParser;
import parser.ParseException;

public class QueryExecutor {

  private static FedStatement fedStatement;
  /*
   * This map holds the JDBC Statement, it it initialized automatically when
   * FedStatement is initialized.
   */
  private static HashMap<Integer, Statement> statementsMap;

  public static void setStatementsMap(HashMap<Integer, Statement> statements) {
    statementsMap = statements;
  }

  /*
   * Can be used to execute SET command or some other commands like this. Not
   * sure about it's usefullness at the moment.
   */
  public static void execute(String query) {

  }

  public static int executeUpdate(String query)
      throws FedException, ParseException {
    int queryType = QueryTypeConstant.NONE;
    int result = -1;

    /* Some complex preprocess start */

    /*
     * Removes tabs, extra spaces and lines for parser to understand according
     * to the grammar. NOTE: We use this method because skipping tabs, spaces
     * and new lines does not work efficiently.
     */
    boolean isInsertQuery = query.startsWith("INSERT");
    String preserveWhereClause = "";
    if (isInsertQuery) {
      preserveWhereClause = query.substring(query.indexOf("VALUES") + 6);
      query = query.substring(0, query.indexOf("VALUES") + 6);
    }

    query = processQueryForParser(query);

    /* Some complex preprocess end */

    // Every query needs ';' to parse, so being added here.
    // Parsing starts here.
    GepardParser parser = new GepardParser(convertToParsableQuery(query + ";"));

    /* If a query is SET or ALTER, do not parse and execute it directly */
    if (!shouldNotParse(query))
      // This method is a general method from where all grammar starts.
      queryType = parser.ParseQuery();

    /* Some complex post process start before going to database */

    /*
     * Special characters like umlauts were replaced with unicode equivalents
     * while running processQueryForParser method. Reason: JavaCC does not
     * support umlauts
     */
    query = UnicodeManager.replaceUnicodesWithChars(query);
    query = replace3DashesWithSpace(query);
    query = replaceBraces(query);

    if (isInsertQuery) {
      query += preserveWhereClause;
    }
    /* Some complex post process end before going to database */

    switch (queryType) {
      case QueryTypeConstant.CREATE_NON_PARTITIONED:
	result = createTable(query);
	break;
      case QueryTypeConstant.CREATE_PARTITIONED:
	result = createTableHorizontal(query);
	break;
      case QueryTypeConstant.DROP:
	result = dropTable(query);
	break;
      case QueryTypeConstant.DELETE:
	result = deleteFromTable(query);
	break;
      case QueryTypeConstant.INSERT:
	result = insertIntoTable(query);
	break;
      default:
	result = executeDefaultQuery(query);
    }
    return result;
  }

  private static String replaceBraces(String query) {
    query = query.replaceAll("[(]{3}", "(");
    query = query.replaceAll("//////", ")");
    return query;
  }

  /*
   * Was added to parse successfully because I am unable to handle space in
   * between string constant in Parser
   */
  private static String replace3DashesWithSpace(String query) {
    Pattern pattern = Pattern.compile("'(.+[---]+.+)'");
    Matcher m = pattern.matcher(query);
    while (m.find()) {
      String searchStr = m.group();
      query = query.replaceAll(searchStr, searchStr.replaceAll("---", " "));
    }
    return query;
  }

  private static String processQueryForParser(String query) {
    StringBuilder sb = new StringBuilder(query);

    List<Pattern> patterns = new ArrayList<Pattern>();
    patterns.add(Pattern.compile("\t"));
    patterns.add(Pattern.compile("\n"));
    patterns.add(Pattern.compile("\r"));

    /*
     * There are so many doubles spaces, so some of them are not replaced, thus
     * creating problem for parser, so we want to check as much as 10 times to
     * make sure the query is finely formatted.
     */
    int counter = 10;
    while (counter > 0) {
      patterns.add(Pattern.compile("  "));
      counter--;
    }

    for (Pattern pattern : patterns) {
      Matcher m = pattern.matcher(sb);
      sb = new StringBuilder(m.replaceAll(" "));
    }

    query = sb.toString();

    /*
     * Removes all spaces in string constant because they are redundant e.g.
     * 'ABC ' become 'ABC'
     */
    Pattern pattern = Pattern.compile("'([^',]*[ ]+)'");
    Matcher m = pattern.matcher(query);
    while (m.find()) {
      String searchStr = m.group();
      query = query.replaceAll(searchStr, searchStr.replaceAll(" ", ""));
    }

    /*
     * Replaces spaces in between string constant with ___ (with replace back)
     * e.g. 'ABC XYZ' becomes 'ABC___XYZ'. It is done because I am unable to
     * handle space between string constant parser.
     */
    pattern = Pattern.compile("'([^',]*[ ]+.+)'");
    m = pattern.matcher(query);
    while (m.find()) {
      String searchStr = m.group();
      query = query.replaceAll(searchStr, searchStr.replaceAll(" ", "---"));
    }

    /*
     * Replaces ( and ) because ) is showing conflict in parser and can not add
     * it in String constant, so workaround.
     */
    pattern = Pattern.compile("'.*[^,][(].*[)].*'");
    m = pattern.matcher(query);
    while (m.find()) {
      String searchStr = m.group();
      query = query.replaceAll(searchStr,
	  searchStr.replaceAll("[(]{1}", "((("));
      query = query.replaceAll(searchStr, searchStr.replaceAll(")", "//////"));
    }

    /*
     * Replaces umlauts with unicodes to parse successfully because JavaCC
     * replaces umlauts with unicodes too
     */
    query = UnicodeManager.getUnicodedQuery(query);
    return query;
  }

  private static boolean shouldNotParse(String query) {
    return query.startsWith("SET") || query.startsWith("ALTER");
  }

  private static int executeDefaultQuery(String query) throws FedException {
    int result = -1;
    String connectionDB = "";
    Integer connectionNumber = -1;
    Statement statement = null;
    // Simple SET query, i.e. set echo on, will be ignored
    if (query.toUpperCase().startsWith("SET"))
      return 0;
    CustomLogger.log(Level.INFO, "Received FJDBC: " + query);
    try {
      for (Integer statementKey : statementsMap.keySet()) {
	connectionNumber = statementKey;
	if (statementKey == 1) {
	  connectionDB = ConnectionConstants.CONNECTION_1_SID;
	}
	if (statementKey == 2) {
	  connectionDB = ConnectionConstants.CONNECTION_2_SID;
	}
	if (statementKey == 3) {
	  connectionDB = ConnectionConstants.CONNECTION_3_SID;
	}
	statement = statementsMap.get(statementKey);
	CustomLogger.log(Level.INFO,
	    "Sending to " + connectionDB + ": " + query);
	result = statement.executeUpdate(query);
      }
    } catch (SQLException e) {
      String message = "Connect " + connectionNumber + " " + connectionDB + ": "
	  + e.getLocalizedMessage();
      CustomLogger.log(Level.SEVERE, "JDBC SQLException in " + connectionDB
	  + ": " + e.getLocalizedMessage());
      throw new FedException(new Throwable(message));
    }
    /*
     * CREATE query is neither INSERT nor UPDATE so it will always return 0 as
     * it effects 0 tuples
     */
    return 0;
  }

  private static int deleteFromTable(String query) throws FedException {
    int result = -1;
    String connectionDB = "";
    int statementKey = 1;
    java.sql.Statement statement = null;
    CustomLogger.log(Level.INFO, "Received FJDBC: " + query);
    // Map of 3 oracle.jdbc.driver.OracleStatement objects
    while (statementKey <= statementsMap.size()) {
      statement = statementsMap.get(statementKey);
      if (statementKey == 1) {
	connectionDB = ConnectionConstants.CONNECTION_1_SID;
      }
      if (statementKey == 2) {
	connectionDB = ConnectionConstants.CONNECTION_2_SID;
      }
      if (statementKey == 3) {
	connectionDB = ConnectionConstants.CONNECTION_3_SID;
      }
      CustomLogger.log(Level.INFO, "Sending to " + connectionDB + ": " + query);
      try {
	result = statement.executeUpdate(query);
	statementKey++;
      } catch (SQLException e) {
	if (e instanceof SQLIntegrityConstraintViolationException) {
	  statementKey++;
	  continue;
	} else if (fedStatement.getConnection().getAutoCommit() == false) {
	  fedStatement.getConnection().rollback();

	  String message = "Connect " + statementKey + " " + connectionDB + ": "
	      + e.getLocalizedMessage();
	  CustomLogger.log(Level.SEVERE, "JDBC SQLException in " + connectionDB
	      + ": " + e.getLocalizedMessage());
	  throw new FedException(new Throwable(message));
	}
	e.printStackTrace();
      }

    }
    return result;
  }

  private static int insertIntoTable(String query) throws FedException {
    int result = -1;
    String connectionDB = "";
    int statementKey = 1;

    Statement statement = null;
    // Logger: redundant, was called earlier in executeUpdate
    // CustomLogger.log(Level.INFO, "Received FJDBC: " + query);
    while (statementKey <= statementsMap.size()) {
      statement = statementsMap.get(statementKey);
      if (statementKey == 1) {
	connectionDB = ConnectionConstants.CONNECTION_1_SID;
      }
      if (statementKey == 2) {
	connectionDB = ConnectionConstants.CONNECTION_2_SID;
      }
      if (statementKey == 3) {
	connectionDB = ConnectionConstants.CONNECTION_3_SID;
      }

      try {
	CustomLogger.log(Level.INFO,
	    "Sending to " + connectionDB + ": " + query);
	result = statement.executeUpdate(query);
	statementKey++;
      } catch (SQLException e) {
	if (e instanceof SQLIntegrityConstraintViolationException) {
	  statementKey++;
	  continue;
	} else if (fedStatement.getConnection().getAutoCommit() == false) {
	  fedStatement.getConnection().rollback();
	  String message = "Connect " + statementKey + " " + connectionDB + ": "
	      + e.getLocalizedMessage();
	  CustomLogger.log(Level.SEVERE, "JDBC SQLException in " + connectionDB
	      + ": " + e.getLocalizedMessage());
	  throw new FedException(new Throwable(message));
	}
	e.printStackTrace();
      }
    }
    return result;
  }

  private static int createTable(String query) throws FedException {
    String connectionDB = "";
    Integer connectionNumber = -1;
    boolean hasException = false;
    String exceptionMessage = "";

    CustomLogger.log(Level.INFO, "Received FJDBC: " + query);
    Statement statement = null;
    for (Integer statementKey : statementsMap.keySet()) {
      connectionNumber = statementKey;

      if (statementKey == 1) {
	connectionDB = ConnectionConstants.CONNECTION_1_SID;
      }
      if (statementKey == 2) {
	connectionDB = ConnectionConstants.CONNECTION_2_SID;
      }
      if (statementKey == 3) {
	connectionDB = ConnectionConstants.CONNECTION_3_SID;
      }
      statement = statementsMap.get(statementKey);

      try {
	CustomLogger.log(Level.INFO,
	    "Sending to " + connectionDB + ": " + query);
	statement.executeUpdate(query);
      } catch (Exception e) {
	String message = "Connect " + connectionNumber + " " + connectionDB
	    + ": " + e.getLocalizedMessage();
	CustomLogger.log(Level.SEVERE, "JDBC SQLException in " + connectionDB
	    + ": " + e.getLocalizedMessage());
	hasException = true;
	exceptionMessage = e.getMessage();
      }
    }

    if (hasException)
      throw new FedException(new Throwable(exceptionMessage));

    // CREATE query is neither INSERT nor UPDATE so it will always return 0
    return 0;
  }

  private static int createTableHorizontal(String query) throws FedException {
    Statement statementOfDB1 = statementsMap.get(1);
    Statement statementOfDB2 = statementsMap.get(2);
    Statement statementOfDB3 = statementsMap.get(3);

    /*
     * Sets true if the Create query has to be deployed on first 2 DBs, that
     * means the list_of_boundaries for Horizontal Partitioning has only 1
     * boundary
     */
    boolean createFewerPartitionsThanDBs = createFewerPartitionsThanDBs(query);

    String queryForDB1 = buildPartitionedQueryForDB1(query,
	createFewerPartitionsThanDBs);
    String queryForDB2 = buildPartitionedQueryForDB2(query,
	createFewerPartitionsThanDBs);
    String queryForDB3 = buildPartitionedQueryForDB3(query,
	createFewerPartitionsThanDBs);

    /*
     * Taking advantage to form query from DB3 to DB2 when there is only one
     * boundary provided in list_of_boundaries for Horizontal Partitioning .
     */
    if (createFewerPartitionsThanDBs) {
      queryForDB2 = queryForDB3;
    }
    String fdbsCreated = "Query created by FDBS layer: ";
    try {
      CustomLogger.log(Level.INFO, fdbsCreated + queryForDB1
	  .replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
      CustomLogger.log(Level.INFO,
	  "Sending to:" + ConnectionConstants.CONNECTION_1_SID + ": "
	      + queryForDB1.replaceAll("  ", " ").replaceAll("\r\n", " ")
		  .replaceAll("\t", " "));
      statementOfDB1.executeUpdate(queryForDB1);
    } catch (SQLException e) {
      CustomLogger.log(Level.SEVERE,
	  "JDBC SQLException in " + ConnectionConstants.CONNECTION_1_SID + ": "
	      + e.getLocalizedMessage());
    }
    try {
      CustomLogger.log(Level.INFO, fdbsCreated + queryForDB2
	  .replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
      CustomLogger.log(Level.INFO,
	  "Sending to: " + ConnectionConstants.CONNECTION_2_SID + ": "
	      + queryForDB2.replaceAll("  ", " ").replaceAll("\r\n", " ")
		  .replaceAll("\t", " "));
      statementOfDB2.executeUpdate(queryForDB2);
    } catch (SQLException e) {
      CustomLogger.log(Level.SEVERE,
	  "JDBC SQLException in " + ConnectionConstants.CONNECTION_2_SID + ": "
	      + e.getLocalizedMessage());
    }
    try {
      if (!createFewerPartitionsThanDBs) {
	CustomLogger.log(Level.INFO,
	    fdbsCreated + queryForDB3.replaceAll("  ", " ")
		.replaceAll("\r\n", " ").replaceAll("\t", " "));
	CustomLogger.log(Level.INFO,
	    "Sending to: " + ConnectionConstants.CONNECTION_3_SID + ": "
		+ queryForDB3.replaceAll("  ", " ").replaceAll("\r\n", " ")
		    .replaceAll("\t", " "));
	statementOfDB3.executeUpdate(queryForDB3);
      }
    } catch (SQLException e) {
      CustomLogger.log(Level.SEVERE,
	  "Failed to send to " + ConnectionConstants.CONNECTION_3_SID + ": "
	      + e.getLocalizedMessage());
    }

    // CREATE query is neither INSERT nor UPDATE so it will always return 0
    return 0;
  }

  /**
   * This method checks whether the list_of_boundaries provided in Horizontal
   * Partitioning has only 1 boundary. Returns true for 1 element.
   * 
   * @param query
   */
  private static boolean createFewerPartitionsThanDBs(String query) {
    boolean createFewerPartitionsThanDBs = false;
    String columnName = query.substring(
	query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
	query.lastIndexOf("("));
    // Fetching range
    int firstIndex = query.lastIndexOf(columnName + "(")
	+ (columnName.length() + 1);
    int secondIndex = query.lastIndexOf(",");

    /*
     * "True" means only 1 boundary provided for horizontal partitioning, so
     * table should be created in first 2 DBs.
     */
    if (secondIndex < firstIndex) {
      secondIndex = query.lastIndexOf("))");
      createFewerPartitionsThanDBs = true;
    }
    return createFewerPartitionsThanDBs;
  }

  private static String buildPartitionedQueryForDB1(String query,
      boolean createFewerPartitionsThanDBs) {
    StringBuffer executableQuery = new StringBuffer();
    StringBuffer basicQuery = new StringBuffer(
	query.substring(0, query.indexOf("HORIZONTAL")));

    // Removes last ')' to further append constraint
    basicQuery = new StringBuffer(
	basicQuery.substring(0, basicQuery.lastIndexOf(")")));
    basicQuery.append(", constraint ");

    // Get values from the Query to build a constraint
    String tableName = query.substring("CREATE TABLE ".length(),
	query.indexOf(" ", "CREATE TABLE ".length()));
    String columnName = query.substring(
	query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
	query.lastIndexOf("("));

    // Fetching range
    int firstIndex = query.lastIndexOf(columnName + "(")
	+ (columnName.length() + 1);
    int secondIndex = query.lastIndexOf(",");

    // If true it means it has one list of attribute for horizontal
    // partitioning
    if (createFewerPartitionsThanDBs) {
      secondIndex = query.lastIndexOf("))");
    }

    String maxRange = query.substring(firstIndex, secondIndex);

    // Appends constraint name
    basicQuery.append(tableName + "_" + columnName + "_HORIZ check (");
    basicQuery.append(columnName + " <= " + maxRange);
    basicQuery.append(")");

    // Adds back ')' after constraint is appended
    executableQuery = basicQuery.append(")");

    return executableQuery.toString();
  }

  private static String buildPartitionedQueryForDB2(String query,
      boolean createFewerPartitionsThanDBs) {
    if (createFewerPartitionsThanDBs) {
      return "";
    }

    StringBuffer executableQuery = new StringBuffer();
    StringBuffer basicQuery = new StringBuffer(
	query.substring(0, query.indexOf("HORIZONTAL")));

    // Removes last ')' to further append constraint
    basicQuery = new StringBuffer(
	basicQuery.substring(0, basicQuery.lastIndexOf(")")));

    basicQuery.append(", constraint ");

    // Get values from Query to build constraint
    String tableName = query.substring("CREATE TABLE ".length(),
	query.indexOf(" ", "CREATE TABLE ".length()));
    String columnName = query.substring(
	query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
	query.lastIndexOf("("));
    String lowerRange = query.substring(
	query.indexOf(columnName + "(") + (columnName.length() + 1),
	query.lastIndexOf(","));
    String upperRange = query.substring(
	query.indexOf(lowerRange + ",") + (lowerRange + ",").length(),
	query.lastIndexOf("))"));

    // Appends constraint name
    basicQuery.append(tableName + "_" + columnName + "_HORIZ check (");
    basicQuery.append(columnName);
    basicQuery.append(" between " + (Integer.parseInt(lowerRange) + 1) + " and " + upperRange);
    basicQuery.append(")");

    // Adds back ')' after constraint is appended
    executableQuery = basicQuery.append(")");

    return executableQuery.toString();
  }

  private static String buildPartitionedQueryForDB3(String query,
      boolean createFewerPartitionsThanDBs) {
    StringBuffer executableQuery = new StringBuffer();
    StringBuffer basicQuery = new StringBuffer(
	query.substring(0, query.indexOf("HORIZONTAL")));

    String operator = "";
    if (createFewerPartitionsThanDBs) {
      operator = " >= ";
    } else {
      operator = " > ";
    }

    // Removes last ')' to further append constraint
    basicQuery = new StringBuffer(
	basicQuery.substring(0, basicQuery.lastIndexOf(")")));

    basicQuery.append(", constraint ");

    // Get values from Query to build constraint
    String tableName = query.substring("CREATE TABLE ".length(),
	query.indexOf(" ", "CREATE TABLE ".length()));
    String columnName = query.substring(
	query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
	query.lastIndexOf("("));

    String maxRange = "";
    if (createFewerPartitionsThanDBs) {
      maxRange = query.substring(
	  query.indexOf(columnName + "(") + (columnName.length() + 1),
	  query.lastIndexOf("))"));
    } else {
      maxRange = query.substring(query.lastIndexOf(",") + 1,
	  query.lastIndexOf("))"));
    }

    // Appends constraint name
    basicQuery.append(tableName + "_" + columnName + "_HORIZ check (");
    basicQuery.append(columnName + operator + maxRange);
    basicQuery.append(")");

    // Adds back ')' after constraint is appended
    executableQuery = basicQuery.append(")");

    return executableQuery.toString();
  }

  private static int dropTable(String query) throws FedException {
    int result = -1;
    String connectionDB = "";
    Integer connectionNumber = -1;

    /**commented out to fix the issue #2
    CustomLogger.log(Level.INFO,
    "Received FJDBC: " + query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
    */
    try {
      Statement statement = null;
      for (Integer statementKey : statementsMap.keySet()) {
	connectionNumber = statementKey;
	if (statementKey == 1) {
	  connectionDB = ConnectionConstants.CONNECTION_1_SID;
	}
	if (statementKey == 2) {
	  connectionDB = ConnectionConstants.CONNECTION_2_SID;
	}
	if (statementKey == 3) {
	  connectionDB = ConnectionConstants.CONNECTION_3_SID;
	}

	statement = statementsMap.get(statementKey);
	CustomLogger.log(Level.INFO,
	    "Sending to " + connectionDB + ": " + query.replaceAll("  ", " ")
		.replaceAll("\r\n", " ").replaceAll("\t", " "));
	statement.executeUpdate(query);
      }
    } catch (SQLException e) {
      String dbMessage = "JDBC SQLException in " + connectionDB + ": "
	  + e.getMessage();
      CustomLogger.log(Level.SEVERE, dbMessage);
      throw new FedException(new Throwable(dbMessage));
    }
    return result;
  }

  /*
   * Not used yet but might be used in further implementation. Parser requires
   * query as InputStream, so this method converts String queries and returns
   * List of parse-able InputStream queries
   */
  public static List<InputStream> convertToParsableQueries(
      List<String> queries) {
    List<InputStream> parsableQueries = new ArrayList<InputStream>();
    for (int i = 0; i < queries.size(); i++) {
      parsableQueries.add(convertToParsableQuery(queries.get(i)));
    }
    return parsableQueries;
  }

  /*
   * Parser requires query as InputStream, so this method converts String query
   * and returns parse-able InputStream query
   */
  public static InputStream convertToParsableQuery(String query) {
    return new ByteArrayInputStream(query.getBytes());
  }

  public static void setFedStatement(FedStatement statement) {
    fedStatement = statement;
  }

}
