package application;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import fjdbc.FedConnection;
import fjdbc.FedException;
import fjdbc.FedPseudoDriver;
import fjdbc.FedStatement;
import parser.ParseException;
import java.util.logging.*;

public class Application {

	private static long startTime;
	private static long duration;

	public static void main(String[] args) throws ParseException {
		// Selects files and stores scripts in a list
		File selectedFile = FileUtility.getFile();
		List<String> scriptsFromSelectedFile = FileUtility.getScriptsFromFile(selectedFile);
		// Initializing logger
		Logger logger = Logger.getLogger("MyLog");
		FileHandler fh;
		// Setting the format
		System.setProperty("java.util.logging.SimpleFormatter.format", "<%1$tT.%1$tL>%5$s %n");

		try {

			// This block configure the logger with handler and formatter
			fh = new FileHandler("logs/fedprot.txt");
			logger.addHandler(fh);
			// logger.setLevel(Level.ALL);
			SimpleFormatter formatter = new SimpleFormatter();

			fh.setFormatter(formatter);

		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			logger.info("Start FDBS");
			// Gets connection based on Username and Password
			FedConnection fedConnection = new FedPseudoDriver().getConnection(ApplicationConstants.USERNAME,
					ApplicationConstants.PASSWORD);
			fedConnection.setAutoCommit(false);

			FedStatement fedStatement = fedConnection.getStatement();

			// Just a formatting to display result
			OutputFormatter.printAstericks();
			System.out.println("Executing script file \'" + selectedFile.getAbsolutePath() + "\' ...");

			// Time starts
			startTime = System.currentTimeMillis();

			// All scripts taken from file provided run here one by one
			int totalOperations = 0;
			for (String currentScript : scriptsFromSelectedFile) {
				// If the query is DDL or DML, executeUpdate should be called
				// from FJDBC
				if (isDDLOrDMLScript(currentScript.toUpperCase()))
					fedStatement.executeUpdate(currentScript);
				else
					fedStatement.executeQuery(currentScript);

				totalOperations++;
			}

			System.out.println(totalOperations + " operations");

			// Prints Time Taken
			System.out.println(getTimeTaken());
			OutputFormatter.printAstericks();
		} catch (FedException e) {
			System.out.println(e);
		}

	}

	/**
	 * @param script
	 * @return
	 */
	private static boolean isDDLOrDMLScript(String script) {
		return script.startsWith("CREATE") || script.startsWith("DROP") || script.startsWith("INSERT")
				|| script.startsWith("DELETE") || script.startsWith("UPDATE") || script.startsWith("ALTER")
				|| script.startsWith("SET");
	}

	private static String getTimeTaken() {
		duration = System.currentTimeMillis() - startTime;
		Date timeTaken = new Date(duration);
		String timeTakenStr = String.format("Time Taken : %2d Min : %2d Sec : %3d Millis", timeTaken.getMinutes(),
				timeTaken.getSeconds(), (timeTaken.getTime() % 1000));
		return timeTakenStr;
	}

}
