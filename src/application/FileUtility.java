package application;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.filechooser.FileFilter;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.filechooser.FileSystemView;

public class FileUtility {

	public static File getFile() {
		File scriptFile = null;

		JFileChooser jfc = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
		jfc.setFileSelectionMode(JFileChooser.FILES_ONLY);
		FileFilter sql = new FileNameExtensionFilter("SQL Dump File (.sql)", "sql");
		jfc.addChoosableFileFilter(sql);

		int returnValue = jfc.showOpenDialog(null);

		String message = "Only SQL Dump (.sql) file is accepted.\nDo you want to try again!";
		if (returnValue == JFileChooser.APPROVE_OPTION) {
			scriptFile = jfc.getSelectedFile();
			if (!(scriptFile != null && scriptFile.getAbsolutePath().toLowerCase().endsWith(".sql"))) {
				int selectedOption = JOptionPane.showConfirmDialog(null, message, "Choose", JOptionPane.YES_NO_OPTION);
				if (selectedOption == JOptionPane.YES_OPTION) {
					getFile();
				} else {
					System.exit(1);
				}
			}
		} else {
			int selectedOption = JOptionPane.showConfirmDialog(null, message, "Choose", JOptionPane.YES_NO_OPTION);
			if (selectedOption == JOptionPane.YES_OPTION) {
				scriptFile = getFile();
			} else {
				System.exit(1);
			}
		}

		return scriptFile;
	}

	public static List<String> getScriptsFromFile(File file) {
		List<String> scripts = new ArrayList<String>();

		try {
			Scanner read = new Scanner(file, "UTF-8");
			read.useDelimiter(";");

			while (read.hasNext()) {
				String script = read.next().trim();

				// If there is an empty string then do not add as a script
				if (script.isEmpty())
					continue;

				// Remove lines from script that are comments or special lines
				// like -- or /* //
				script = fetchValidScript(script);

				scripts.add(script);
			}
			read.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return scripts;
	}

	
	// Does not consider comments (e.g. -- // /* \\ # as scripts
	private static String fetchValidScript(String script) {
		StringBuffer validScript = new StringBuffer();
		Scanner input = new Scanner(script);
		while (input.hasNextLine()) {
			String line = input.nextLine();
			if (line.startsWith("--") || line.startsWith("\\\\") || line.startsWith("//") || line.startsWith("/*") || line.startsWith("#") || line.trim().isEmpty())
				continue;
			else
				validScript.append(line);
		}
		return validScript.toString();
	}

}
