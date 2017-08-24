package fdbs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
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
	// This map holds the JDBC Statement, it it initialized automatically when
	// FedStatement is initialized.
	private static HashMap<Integer, Statement> statementsMap;

	public static void setStatementsMap(HashMap<Integer, Statement> statements) {
		statementsMap = statements;
	}

	// Can be used to execute SET command or some other commands like this.
	// Not sure about it's usefullness at the moment.
	public static void execute(String query) {

	}

	public static int executeUpdate(String query) throws FedException, ParseException {
		int queryType = QueryTypeConstant.NONE;
		int result = -1;
		query = processQueryForParser(query);

		// Every query needs ';' to parse, so being added here. Parsing starts
		// here.
		GepardParser parser = new GepardParser(convertToParsableQuery(query + ";"));

		// This method should be called to parse all DML queries.
		// If query is SET or ALTER, simply do not parse and execute on the DBs
		if (!shouldNotParse(query.toUpperCase()))
			queryType = parser.ParseQuery();

		query = UnicodeManager.replaceUnicodesWithChars(query);
		
		switch (queryType) {
		case QueryTypeConstant.CREATE_NON_PARTITIONED:
			result = createNonPartitioned(query);
			break;
		case QueryTypeConstant.CREATE_PARTITIONED:
			result = createPartitioned(query);
			break;
		case QueryTypeConstant.DROP:
			result = dropTable(query);
			break;
		case QueryTypeConstant.DELETE:
			result = deleteTable(query);
			break;
		case QueryTypeConstant.INSERT:
			result = insertTable(query);
			break;
		default:
			result = executeDefaultQuery(query);
		}

		return result;

	}

	private static String processQueryForParser(String query) {
		StringBuilder sb = new StringBuilder(query);

		List<Pattern> patterns = new ArrayList<Pattern>();
		patterns.add(Pattern.compile("\t"));
		patterns.add(Pattern.compile("\n"));
		patterns.add(Pattern.compile("\r"));

		// There are many doubles spaces, so we some are left unreplaced thus
		// creating problem for parser, so we want to check as much as 10 times
		// to make sure query is finely formatted.
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
		
		Pattern pattern = Pattern.compile("'([^',]*[ ]+)'");
		Matcher m = pattern.matcher(query);
		while(m.find()) {
			String searchStr = m.group();
			query = query.replaceAll(searchStr, searchStr.replaceAll(" ", ""));
		}
		
		// Replaces umlauts with unicodes to parse successfully because JavaCC replaces umlauts with unicodes too
		query = UnicodeManager.getUnicodedQuery(query);
		
		return query;
	}

	private static boolean shouldNotParse(String query) {
		return query.startsWith("SET") || query.startsWith("ALTER");
	}

	private static int executeDefaultQuery(String query) throws FedException{
		int result = -1;
		String connectionDB = "";
		Integer connectionNumber = -1;
		Statement statement = null;
		// Simple SET query, i.e. set echo on, is not executable from JDBC
		if (query.toUpperCase().startsWith("SET"))
			return 0;

		try {
			for (Integer statementKey : statementsMap.keySet()) {
				// Logger
				CustomLogger.log(Level.INFO, "Received FJDBC: " + query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 
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
				result = statement.executeUpdate(query);
				CustomLogger.log(Level.INFO, "Sent "+connectionDB+": "+ query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
			}
		} catch (SQLException e) {
			String message = "Connect " + connectionNumber + " " + connectionDB + ": " + e.getMessage();
			CustomLogger.log(Level.SEVERE, "Sending failed to " + connectionDB + ": " + e.getMessage());
			throw new FedException(new Throwable(message));
		}

		// CREATE query is neither INSERT nor UPDATE so it will always return 0
		// as it effects 0 tuples
		return 0;
	}

	private static int deleteTable(String query) throws FedException {
		int result = -1;
		String connectionDB = "";
		Integer connectionNumber = -1;

		Statement statement = null;
		try {
			for (Integer statementKey : statementsMap.keySet()) {

				// Logger
				CustomLogger.log(Level.INFO, "Received FJDBC: " + query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 
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
				result = statement.executeUpdate(query);
				CustomLogger.log(Level.INFO, "Sent "+connectionDB+": "+ query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
			}
		} catch (SQLException e) {
			// Rollback if there is an error in any database while deleting
			// table. We can rollback only if autocommit is off, so checking that
			if (fedStatement.getConnection().getAutoCommit() == false)
				fedStatement.getConnection().rollback();

			String message = "Connect " + connectionNumber + " " + connectionDB + ": " + e.getMessage();
			CustomLogger.log(Level.SEVERE, "Sending failed to " + connectionDB + ": " + e.getMessage());
			throw new FedException(new Throwable(message));
			
		}

		return result;
	}

	private static int insertTable(String query) throws FedException {
		int result = -1;
		String connectionDB = "";
		Integer connectionNumber = -1;
		
		Statement statement = null;
		try {
			for (Integer statementKey : statementsMap.keySet()) {

				// Logger
				CustomLogger.log(Level.INFO, "Received FJDBC: " + query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 
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
				result = statement.executeUpdate(query);
				CustomLogger.log(Level.INFO, "Sent "+connectionDB+": "+ query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 
			}
		} catch (SQLException e) {
			// Rollback if there is an error in any database while deleting
			// table. We can rollback only if autocommit is off, so checking that
			if (fedStatement.getConnection().getAutoCommit() == false)
				fedStatement.getConnection().rollback();

			String message = "Connect " + connectionNumber + " " + connectionDB + ": " + e.getMessage();
			CustomLogger.log(Level.SEVERE, "Sending failed to " + connectionDB + ": " + e.getMessage());
			throw new FedException(new Throwable(message));
		}

		return result;
	}

	private static int createNonPartitioned(String query) throws FedException {
		String connectionDB = "";
		Integer connectionNumber = -1;

		try {
			Statement statement = null;
			for (Integer statementKey : statementsMap.keySet()) {
				
				CustomLogger.log(Level.INFO, "Received FJDBC: " + query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 
				
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
				statement.executeUpdate(query);
				
				CustomLogger.log(Level.INFO, "Sent "+connectionDB+": "+ query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 
				
			}
		} catch (SQLException e) {
			String message = "Connect " + connectionNumber + " " + connectionDB + ": " + e.getMessage();
			
			CustomLogger.log(Level.SEVERE, "Sending failed to " + connectionDB + ": " + e.getMessage()); 
			throw new FedException(new Throwable(message));
		}

		// CREATE query is neither INSERT nor UPDATE so it will always return 0
		// as it effects 0 tuples
		return 0;
	}

	private static int createPartitioned(String query) throws FedException {
		Statement statementOfDB1 = statementsMap.get(1);
		Statement statementOfDB2 = statementsMap.get(2);
		Statement statementOfDB3 = statementsMap.get(3);
		
		
		// Sets true if the Create query has to be deployed on first 2 DBs, that
		// means the list of attributes provided in Horizontal Partitioning has
		// 1 attribute only
		boolean createLessPartitionsThanDatabase = createLessPartitionsThanDatabase(query);

		String queryForDB1 = getCreatePartitionedQueryForDB1(query, createLessPartitionsThanDatabase);
		String queryForDB2 = getCreatePartitionedQueryForDB2(query, createLessPartitionsThanDatabase);
		String queryForDB3 = getCreatePartitionedQueryForDB3(query, createLessPartitionsThanDatabase);
		
		// Taking advantage to form query from DB3 to DB2 when the list the
		// attribute provided in Horizontal Partitioning is only one.
		if (createLessPartitionsThanDatabase) {
			queryForDB2 = queryForDB3;
		}

		try {
			CustomLogger.log(Level.INFO, "Received FJDBC:" + queryForDB1.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 
			statementOfDB1.executeUpdate(queryForDB1);
			CustomLogger.log(Level.INFO, "Sent:"+ConnectionConstants.CONNECTION_1_SID+": "+ queryForDB1.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 
		}
		catch (SQLException e) {
			CustomLogger.log(Level.SEVERE, "Sending failed to " + ConnectionConstants.CONNECTION_1_SID + ": " + e.getMessage()); 
		}
		try {
			CustomLogger.log(Level.INFO, "Received FJDBC:" + queryForDB2.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
			statementOfDB2.executeUpdate(queryForDB2);
			CustomLogger.log(Level.INFO, "Sent:"+ConnectionConstants.CONNECTION_2_SID+": "+ queryForDB2.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
		}
		catch (SQLException e) {
				CustomLogger.log(Level.SEVERE, "Sending failed to " + ConnectionConstants.CONNECTION_2_SID + ": " + e.getMessage()); 
		}
		try {	
			if (!createLessPartitionsThanDatabase) {
				CustomLogger.log(Level.INFO, "Received FJDBC:" + queryForDB3.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
				statementOfDB3.executeUpdate(queryForDB3);
				CustomLogger.log(Level.INFO, "Sent:"+ConnectionConstants.CONNECTION_3_SID+": "+ queryForDB3.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
			}
		} catch (SQLException e) {
			CustomLogger.log(Level.SEVERE, "Sending failed to " + ConnectionConstants.CONNECTION_3_SID + ": " + e.getMessage()); 
		}

		// CREATE query is neither INSERT nor UPDATE so it will always return 0
		// as it effects 0 tuples
		return 0;
	}

	/**
	 * This method checks whether the list of attribute provided in Horizontal
	 * Partitioning is one or not. returns true if list of attribute is 1
	 * 
	 * @param query
	 */
	private static boolean createLessPartitionsThanDatabase(String query) {
		boolean createLessPartitionsThanDatabases = false;
		String columnName = query.substring(query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
				query.lastIndexOf("("));
		// Fetching range
		int firstIndex = query.lastIndexOf(columnName + "(") + (columnName.length() + 1);
		int secondIndex = query.lastIndexOf(",");

		// If true it means it has one list of attribute for horizontal
		// partitioning, so two tables should be created in first 2 DBs instead
		// of 3 in all 3 DBs
		if (secondIndex < firstIndex) {
			secondIndex = query.lastIndexOf("))");
			createLessPartitionsThanDatabases = true;
		}

		return createLessPartitionsThanDatabases;
	}

	private static String getCreatePartitionedQueryForDB1(String query, boolean createLessPartitionsThanDatabase) {
		StringBuffer executableQuery = new StringBuffer();
		StringBuffer basicQuery = new StringBuffer(query.substring(0, query.indexOf("HORIZONTAL")));

		// Removes last ')' to further append constraint
		basicQuery = new StringBuffer(basicQuery.substring(0, basicQuery.lastIndexOf(")")));

		basicQuery.append(", constraint ");

		// Get values from Query to build constraint
		String tableName = query.substring("CREATE TABLE ".length(), query.indexOf(" ", "CREATE TABLE ".length()));
		String columnName = query.substring(query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
				query.lastIndexOf("("));

		// Fetching range
		int firstIndex = query.lastIndexOf(columnName + "(") + (columnName.length() + 1);
		int secondIndex = query.lastIndexOf(",");

		// If true it means it has one list of attribute for horizontal
		// partitioning
		if (createLessPartitionsThanDatabase) {
			secondIndex = query.lastIndexOf("))");
		}

		String maxRange = query.substring(firstIndex, secondIndex);

		// Appends constraint name
		basicQuery.append(tableName + "_RANGE_CHK_" + columnName + " check (");
		basicQuery.append(columnName + " < " + maxRange);
		basicQuery.append(")");

		// Adds back ')' after constraint is appended
		executableQuery = basicQuery.append(")");

		return executableQuery.toString();
	}

	private static String getCreatePartitionedQueryForDB2(String query, boolean createLessPartitionsThanDatabase) {
		if (createLessPartitionsThanDatabase) {
			return "";
		}

		StringBuffer executableQuery = new StringBuffer();
		StringBuffer basicQuery = new StringBuffer(query.substring(0, query.indexOf("HORIZONTAL")));

		// Removes last ')' to further append constraint
		basicQuery = new StringBuffer(basicQuery.substring(0, basicQuery.lastIndexOf(")")));

		basicQuery.append(", constraint ");

		// Get values from Query to build constraint
		String tableName = query.substring("CREATE TABLE ".length(), query.indexOf(" ", "CREATE TABLE ".length()));
		String columnName = query.substring(query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
				query.lastIndexOf("("));
		String lowerRange = query.substring(query.indexOf(columnName + "(") + (columnName.length() + 1),
				query.lastIndexOf(","));
		String upperRange = query.substring(query.indexOf(lowerRange + ",") + (lowerRange + ",").length(),
				query.lastIndexOf("))"));

		// Appends constraint name
		basicQuery.append(tableName + "_RANGE_CHK_" + columnName + " check (");
		basicQuery.append(columnName);
		basicQuery.append(" between " + lowerRange + " and " + upperRange);
		basicQuery.append(")");

		// Adds back ')' after constraint is appended
		executableQuery = basicQuery.append(")");

		return executableQuery.toString();
	}

	private static String getCreatePartitionedQueryForDB3(String query, boolean createLessPartitionsThanDatabase) {
		StringBuffer executableQuery = new StringBuffer();
		StringBuffer basicQuery = new StringBuffer(query.substring(0, query.indexOf("HORIZONTAL")));

		String operator = "";
		if (createLessPartitionsThanDatabase) {
			operator = " >= ";
		} else {
			operator = " > ";
		}

		// Removes last ')' to further append constraint
		basicQuery = new StringBuffer(basicQuery.substring(0, basicQuery.lastIndexOf(")")));

		basicQuery.append(", constraint ");

		// Get values from Query to build constraint
		String tableName = query.substring("CREATE TABLE ".length(), query.indexOf(" ", "CREATE TABLE ".length()));
		String columnName = query.substring(query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
				query.lastIndexOf("("));

		String maxRange = "";
		if (createLessPartitionsThanDatabase) {
			maxRange = query.substring(query.indexOf(columnName + "(") + (columnName.length() + 1),
					query.lastIndexOf("))"));
		} else {
			maxRange = query.substring(query.lastIndexOf(",") + 1, query.lastIndexOf("))"));
		}

		// Appends constraint name
		basicQuery.append(tableName + "_RANGE_CHK_" + columnName + " check (");
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

		try {
			Statement statement = null;
			for (Integer statementKey : statementsMap.keySet()) {

				// Logger
				CustomLogger.log(Level.INFO, "Received FJDBC: " + query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 
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
				statement.executeUpdate(query);
				CustomLogger.log(Level.INFO, "Sent "+connectionDB+": "+ query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 

			}
		} catch (SQLException e) {
			String message = "Connect " + connectionNumber + " " + connectionDB + ": " + e.getMessage();
			CustomLogger.log(Level.SEVERE, "Sending failed to " + connectionDB + ": " + e.getMessage()); 
			throw new FedException(new Throwable(message));
		}

		return result;
	}

	// Not used yet but might be used in further implementation
	// Parser requires query as InputStream, so this method converts String
	// queries and returns List of parsable InputStream queries
	public static List<InputStream> convertToParsableQueries(List<String> queries) {
		List<InputStream> parsableQueries = new ArrayList<InputStream>();
		for (int i = 0; i < queries.size(); i++) {
			parsableQueries.add(convertToParsableQuery(queries.get(i)));
		}
		return parsableQueries;
	}

	// Parser requires query as InputStream, so this method converts String
	// query and returns parsable InputStream query
	public static InputStream convertToParsableQuery(String query) {
		return new ByteArrayInputStream(query.getBytes());
	}

	public static void setFedStatement(FedStatement statement) {
		fedStatement = statement;
	}

}
