package fjdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import fdbs.QueryExecutor;
import parser.ParseException;

public class FedStatement implements FedStatementInterface {

	// Map of JDBC Statements
	private HashMap<Integer, Statement> statementsMap;

	private FedConnection connection;

	private boolean isClose = true;

	public FedStatement(FedConnection connection, HashMap<Integer, Statement> statementsMap) {
		this.connection = connection;
		this.statementsMap = statementsMap;
		isClose = false;

		// Initialize query executor to use JDBC statements
		QueryExecutor.setStatementsMap(statementsMap);
	}

	
	public int executeUpdate(String query) throws FedException {
		int result = -1;
		if (isClose) {
			throw new FedException(new Throwable("FedStatement resource is closed."));
		}

		try {
			result = QueryExecutor.executeUpdate(query);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}

	
	public FedConnection getConnection() throws FedException {
		if (isClose) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
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
			throw new FedException(new Throwable(e.getMessage()));
		}
	}

	/*
	 * 
	 * Not used yet, might/might not needs to be changed. So can't comment on it
	 * at the moment.
	 */
	
	public FedResultSet executeQuery(String sql) throws FedException {
		if (isClose) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
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
			throw new FedException(new Throwable(e.getMessage()));
		}
		return fedResultSet;
	}

}
