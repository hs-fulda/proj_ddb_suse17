package fdbs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import fjdbc.FedException;
import parser.GepardParser;
import parser.ParseException;

public class QueryExecutor {

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

		// TEMPORARY: Will be replaced by parseDML after DML portion for parsing
		// is done.
		// It validates or throws Exception. Also returns queryType
		// Every parser Method should return specific number after successful
		// validation which will help us to process the query
		queryType = parser.CreateTable();

		// This method should be called to parse all DML queries. It is not
		// implemented yet queryType = parser.parseDML();

		switch (queryType) {
		case QueryTypeConstant.CREATE_NON_PARTITIONED:
			result = createNonPartitioned(query);
			break;
		case QueryTypeConstant.CREATE_PARTITIONED:
			result = createPartitioned(query);
			break;
		case QueryTypeConstant.DROP:
			result = createPartitioned(query);
			break;
		}

		return result;

	}

	private static int createNonPartitioned(String query) throws FedException {
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

	// To Do: Query for 1 list of attribute boundary (in horizontal partitioning) is not handled yet!
	private static int createPartitioned(String query) throws FedException {
		Statement statementOfDB1 = statementsMap.get(1);
		Statement statementOfDB2 = statementsMap.get(2);
		Statement statementOfDB3 = statementsMap.get(3);

		String queryForDB1 = getCreatePartitionedQueryForDB1(query);
		String queryForDB2 = getCreatePartitionedQueryForDB2(query);
		String queryForDB3 = getCreatePartitionedQueryForDB3(query);

		try {
			statementOfDB1.executeUpdate(queryForDB1);
			statementOfDB2.executeUpdate(queryForDB2);
			statementOfDB3.executeUpdate(queryForDB3);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		// CREATE query is neither INSERT nor UPDATE so it will always return 0
		// as it effects 0 tuples
		return 0;
	}

	private static String getCreatePartitionedQueryForDB1(String query) {
		StringBuffer executableQuery = new StringBuffer();
		StringBuffer basicQuery = new StringBuffer(query.substring(0, query.indexOf("HORIZONTAL")));

		// Removes last ')' to further append constraint
		basicQuery = new StringBuffer(basicQuery.substring(0, basicQuery.lastIndexOf(")")));

		basicQuery.append(", constraint ");

		// Get values from Query to build constraint
		String tableName = query.substring("CREATE TABLE ".length(), query.indexOf(" ", "CREATE TABLE ".length()));
		String columnName = query.substring(query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
				query.lastIndexOf("("));

		String maxRange = query.substring(query.indexOf(columnName + "(") + (columnName.length() + 1),
				query.lastIndexOf(","));

		// Appends constraint name
		basicQuery.append(tableName + "_RANGE_CHK_" + columnName + " check (");
		basicQuery.append(columnName + " <= " + maxRange);
		basicQuery.append(")");

		// Adds back ')' after constraint is appended
		executableQuery = basicQuery.append(")");

		return executableQuery.toString();
	}

	private static String getCreatePartitionedQueryForDB2(String query) {
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
		basicQuery.append(tableName + "_RANGE_CHK" + columnName + " check (");
		basicQuery.append(columnName);
		basicQuery.append(" between " + lowerRange + " and " + upperRange);
		basicQuery.append(")");

		// Adds back ')' after constraint is appended
		executableQuery = basicQuery.append(")");

		return executableQuery.toString();
	}

	private static String getCreatePartitionedQueryForDB3(String query) {
		StringBuffer executableQuery = new StringBuffer();
		StringBuffer basicQuery = new StringBuffer(query.substring(0, query.indexOf("HORIZONTAL")));

		// Removes last ')' to further append constraint
		basicQuery = new StringBuffer(basicQuery.substring(0, basicQuery.lastIndexOf(")")));

		basicQuery.append(", constraint ");

		// Get values from Query to build constraint
		String tableName = query.substring("CREATE TABLE ".length(), query.indexOf(" ", "CREATE TABLE ".length()));
		String columnName = query.substring(query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
				query.lastIndexOf("("));

		String maxRange = query.substring(query.indexOf(columnName + "(") + (columnName.length() + 1),
				query.lastIndexOf(","));

		// Appends constraint name
		basicQuery.append(tableName + "_RANGE_CHK" + columnName + " check (");
		basicQuery.append(columnName + " >= " + maxRange);
		basicQuery.append(")");

		// Adds back ')' after constraint is appended
		executableQuery = basicQuery.append(")");

		return executableQuery.toString();
	}

	// Not used yet but might be used in further implementation
	// Parser requires query as InputStream, so this method converts String queries and returns List of parsable InputStream queries
	public static List<InputStream> convertToParsableQueries(List<String> queries) {
		List<InputStream> parsableQueries = new ArrayList<InputStream>();
		for (int i = 0; i < queries.size(); i++) {
			parsableQueries.add(new ByteArrayInputStream(queries.get(i).getBytes()));
		}
		return parsableQueries;
	}

	// Parser requires query as InputStream, so this method converts String query and returns parsable InputStream query
	public static InputStream convertToParsableQuery(String query) {
		return new ByteArrayInputStream(query.getBytes());
	}

}
