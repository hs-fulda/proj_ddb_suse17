package fdbs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import fjdbc.FedException;
import parser.GepardParser;
import parser.ParseException;

public class QueryExecutor {
	private static Logger logger = Logger.getLogger("MyLog");
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

		// Every query needs ';' to parse, so being added here. Parsing starts
		// here.
		GepardParser parser = new GepardParser(convertToParsableQuery(query + ";"));

		// This method should be called to parse all DML queries.
		queryType = parser.ParseQuery();

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
		}

		return result;

	}

	private static int deleteTable(String query) {
		return 0;
	}

	private static int insertTable(String query) {
		return 0;
	}

	private static int createNonPartitioned(String query) throws FedException {
		
		try {
			Statement statement = null;
			for (Integer statementKey : statementsMap.keySet()) {
				statement = statementsMap.get(statementKey);
				statement.executeUpdate(query);
				
				// Logger
				String connectionDB = "";
				if(statementKey == 1) {
					connectionDB = ConnectionConstants.CONNECTION_1_SID;
				}
				if(statementKey == 2) {
					connectionDB = ConnectionConstants.CONNECTION_2_SID;
				}
				if(statementKey == 3) {
					connectionDB = ConnectionConstants.CONNECTION_3_SID;
				}
				logger.info("Received FJDBC: " + query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " ")); 
				logger.info("Sent "+connectionDB+": "+ query.replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
			}
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
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
			statementOfDB1.executeUpdate(queryForDB1);
			statementOfDB2.executeUpdate(queryForDB2);
			if (!createLessPartitionsThanDatabase) {
				statementOfDB3.executeUpdate(queryForDB3);
			}
		} catch (SQLException e) {
			e.printStackTrace();
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
		basicQuery.append(columnName + " <= " + maxRange);
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
			operator = " > ";
		} else {
			operator = " >= ";
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

		try {
			for (Statement statement : statementsMap.values()) {
				result = statement.executeUpdate(query);
			}
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
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

}
