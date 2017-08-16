package fjdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;

import application.ApplicationConstants;
import fdbs.ConnectionConstants;

public class FedPseudoDriver implements FedPseudoDriverInterface {

	// Gets FedConnection. FedConnection has 3 Database Connections in a Map
	public FedConnection getConnection(String userName, String password) throws FedException {
		FedConnection connection = null;
		try {
			HashMap<Integer, Connection> connectionsMap = new HashMap<Integer, Connection>();

			connectionsMap.put(1,
					DriverManager.getConnection(ConnectionConstants.CONNECTION_1_URL, userName, password));
			connectionsMap.put(2,
					DriverManager.getConnection(ConnectionConstants.CONNECTION_2_URL, userName, password));
			connectionsMap.put(3,
					DriverManager.getConnection(ConnectionConstants.CONNECTION_3_URL, userName, password));

			connection = new FedConnection(connectionsMap);
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
		return connection;
	}

}