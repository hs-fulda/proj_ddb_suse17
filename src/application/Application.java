package application;

import java.io.File;
import java.util.Date;
import java.util.List;

import fjdbc.FedConnection;
import fjdbc.FedException;
import fjdbc.FedPseudoDriver;
import fjdbc.FedStatement;
import parser.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
	private final static Logger logger = LoggerFactory.getLogger(FedPseudoDriver.class);

	private static long startTime;
	private static long duration;

	public static void main(String[] args) throws ParseException {
		//Logger logger = LoggerFactory.getLogger(Application.class);
		String msg;
		// Selects a script file and reads statements into a list
		logger.info("Started the main.");
		logger.info("Reading a script file with SQL statements...");
		File selectedFile = FileUtility.getFile();
		List<String> scriptsFromSelectedFile = FileUtility.getScriptsFromFile(selectedFile);
		logger.info("Reading complete.");

		try {

			// Gets connection based on Username and Password
			FedConnection fedConnection = new FedPseudoDriver().getConnection(ApplicationConstants.USERNAME,
					ApplicationConstants.PASSWORD);
			fedConnection.setAutoCommit(false);

			FedStatement fedStatement = fedConnection.getStatement();

			// Just a formatting to display result
			OutputFormatter.printAstericks();
			msg = "Executing script file \'" + selectedFile.getAbsolutePath() + "\' ...";
			System.out.println(msg);
			CustomLogger.printLog(msg);
			// Time starts
			startTime = System.currentTimeMillis();

			// All scripts taken from file provided run here one by one
			int totalOperations = 0;

			for (String currentScript : scriptsFromSelectedFile) {
				currentScript = currentScript.toUpperCase().trim();
				if (currentScript.isEmpty()) {
					continue;
				}

				// If the query is DDL or DML, executeUpdate should be called
				// from FJDBC
				if (isDDLOrDMLScript(currentScript)) {
					fedStatement.executeUpdate(currentScript);
					CustomLogger.printLog(currentScript);
				} else {
					fedStatement.executeQuery(currentScript);
					CustomLogger.printLog(currentScript);
				}

				totalOperations++;
			}

			System.out.println(totalOperations + " operations");

			// Prints Time Taken
			System.out.println(getTimeTaken());
			OutputFormatter.printAstericks();
		} catch (FedException e) {
			e.printStackTrace();
		}

	}

	/**
	 * @param script
	 * @return
	 */
	private static boolean isDDLOrDMLScript(String script) {
		return script.startsWith("CREATE") || script.startsWith("DROP") || script.startsWith("INSERT")
				|| script.startsWith("DELETE") || script.startsWith("UPDATE");
	}

	private static String getTimeTaken() {
		duration = System.currentTimeMillis() - startTime;
		Date timeTaken = new Date(duration);
		String timeTakenStr = String.format("Time Taken : %2d Min : %2d Sec : %3d Millis", timeTaken.getMinutes(),
				timeTaken.getSeconds(), (timeTaken.getTime() % 1000));
		return timeTakenStr;
	}

}
