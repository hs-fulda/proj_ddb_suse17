package fjdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class FedConnection implements FedConnectionInterface {

	private HashMap<Integer, Connection> connectionsMap;

	private boolean autoCommit = true;
	private boolean close = true;

	private FedStatement statement;

	public FedConnection(HashMap<Integer, Connection> connectionsMap) {
		this.connectionsMap = connectionsMap;
		close = false;

		// FedConnection has to return FedStatement, so initializing it as soon
		// as FedConnection object is created
		initializeFedStatement();
	}

	private void initializeFedStatement() {
		HashMap<Integer, Statement> statements = new HashMap<Integer, Statement>();

		// Creating JDBC Statements from JDBC Connection Map to prepare
		// FedStatement object
		int i = 0;
		for (Connection connection : connectionsMap.values()) {
			try {
				statements.put(++i, connection.createStatement());
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		statement = new FedStatement(this, statements);
	}

		public void setAutoCommit(boolean commit) throws FedException {
		if (close) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
		}

		try {
			for (Connection connection : connectionsMap.values()) {
				connection.setAutoCommit(commit);
			}
			autoCommit = commit;
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
	}

	
	public boolean getAutoCommit() throws FedException {
		if (close) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
		}

		return autoCommit;
	}

	public void commit() throws FedException {
		if (close) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
		}
		try {
			for (Connection connection : connectionsMap.values()) {
				connection.commit();
			}
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
	}

	public void rollback() throws FedException {
		if (close) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
		}

		try {
			for (Connection connection : connectionsMap.values()) {
				connection.rollback();
			}
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
	}

	public void close() throws FedException {
		try {
			for (Connection connection : connectionsMap.values()) {
				connection.close();
			}
			close = true;
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
	}

	
	public FedStatement getStatement() {
		return statement;
	}

}
