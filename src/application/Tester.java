package application;

import fjdbc.FedConnection;
import fjdbc.FedException;
import fjdbc.FedPseudoDriver;
import fjdbc.FedResultSet;
import fjdbc.FedStatement;

public class Tester {
	private static final String USER = ApplicationConstants.USERNAME;
	private static final String PASS = ApplicationConstants.PASSWORD;

	private static FedConnection fedConnection = null;
	private static FedStatement fedStatement = null;

	public static void main(String[] args) throws FedException {

		System.out.println("Starting FJDBC.");
		try {
			fedConnection = (new FedPseudoDriver()).getConnection(USER, PASS);
			fedConnection.setAutoCommit(false);
			fedStatement = fedConnection.getStatement();
		} catch (FedException fedException) {
			System.out.println(fedException.getMessage());
			System.out.flush();
		}
		String query;
		int test = 3;

		switch (test) {
		case 1:
			/* EXECUTE THE InsERT QUERY */
			query = "InsERT iNTo paSSenger1K VALUES (1002, 'name2', 'LASTname2', 'gB')";
			int count = 0;
			try {
				count = fedStatement.executeUpdate(query);
				fedConnection.commit();
				System.out.println(count + " tuples inserted");
			} catch (FedException fe) {
				System.err.println(fe.getMessage());
				System.err.println(query);
			}

		case 2:
			/* EXECUTE THE FEDERATED SELECT */
			query = "SeLeCT * FrOm passenger1k WhErE PNR > 1000";
			FedResultSet fedResultSet;
			FedStatement fedStatement;
			try {
				fedStatement = fedConnection.getStatement();
				fedResultSet = fedStatement.executeQuery(query);

				for (int i = 1; i <= fedResultSet.getColumnCount(); i++) {
					System.out.printf("%-15s", fedResultSet.getColumnName(i));
				}
				System.out.println();
				while (fedResultSet.next()) {
					for (int i = 1; i <= fedResultSet.getColumnCount(); i++) {
						System.out.printf("%-15s", fedResultSet.getString(i));
					}
					System.out.println();
				}
				System.out.println();
			} catch (FedException fe) {
				System.err.println(fe.getMessage());
				System.err.println(query);
			}

		case 3:
			/* 1.6 Table or column names: SQL keywords like SELECT, INSERT, etc. are not allowed.  */
			query = "CREATE TABLE WRONG (FETCH VARCHAR(1))";
			try {
				fedStatement = fedConnection.getStatement();
				fedResultSet = fedStatement.executeQuery(query);
			} catch (FedException fe) {
				System.err.println(fe.getMessage());
				System.err.println(query);
			}
		}
		
		/* PROGRAM ENDS */
		try {
			if (fedConnection != null) {
				fedConnection.close();
				System.out.println("fedConnection was closed");
				System.out.flush();
			}
		} catch (FedException fedException) {
			fedException.printStackTrace();
			System.err.println(fedException.getMessage());
			System.out.flush();
			System.err.flush();
		}
	}
}
