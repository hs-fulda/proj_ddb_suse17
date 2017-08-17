/*@ author: Samiul Jahan	*/
package application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomLogger {
	public static void main(String[] args) {
		printLog("Printing Started");
	}

	public static void printLog(String msg) {
		Logger logger = LoggerFactory.getLogger(CustomLogger.class);
		logger.info(msg);
	}
}
