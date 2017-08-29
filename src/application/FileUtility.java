package application;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
    File sqlFile = null;

    JFileChooser jfc = new JFileChooser(
	FileSystemView.getFileSystemView().getHomeDirectory());
    jfc.setFileSelectionMode(JFileChooser.FILES_ONLY);
    FileFilter sql = new FileNameExtensionFilter("SQL Dump File (.sql)", "sql");
    jfc.addChoosableFileFilter(sql);

    int returnValue = jfc.showOpenDialog(null);

    String message = "Only SQL Dump (.sql) file is accepted.\nDo you want to try again?";
    if (returnValue == JFileChooser.APPROVE_OPTION) {
      sqlFile = jfc.getSelectedFile();
      if (!(sqlFile != null
	  && sqlFile.getAbsolutePath().toLowerCase().endsWith(".sql"))) {
	int selectedOption = JOptionPane.showConfirmDialog(null, message,
	    "Choose", JOptionPane.YES_NO_OPTION);
	if (selectedOption == JOptionPane.YES_OPTION) {
	  getFile();
	} else {
	  System.exit(1);
	}
      }
    } else {
      int selectedOption = JOptionPane.showConfirmDialog(null, message,
	  "Choose", JOptionPane.YES_NO_OPTION);
      if (selectedOption == JOptionPane.YES_OPTION) {
	sqlFile = getFile();
      } else {
	System.exit(1);
      }
    }

    return sqlFile;
  }

  public static List<String> getStatementsFromFile(File file) {
    List<String> sQLStatements = new ArrayList<String>();

    BufferedReader br = null;
    try {
      StringBuilder script = new StringBuilder();
      String line;

      br = new BufferedReader(new FileReader(file));

      while ((line = br.readLine()) != null) {
	line = fetchValidLine(line);

	if (line.trim().isEmpty())
	  continue;

	if (line.contains(";")) {
	  String[] strArr = line.split(";");
	  for (String str : strArr) {
	    if (str.isEmpty())
	      continue;

	    script.append("  " + str);
	  }

	  if (script.length() > 0) {
	    sQLStatements.add(script.toString().trim());
	  }
	  script = new StringBuilder();
	} else {
	  // @author: Jahan. Added a blank space to create a proper sql statement  
	  script.append("  " + line);
	}
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
	if (br != null)
	  br.close();
      } catch (IOException ex) {
	ex.printStackTrace();
      }
    }

    return sQLStatements;
  }

  // Does not consider comments (e.g. -- // /* \\ # as scripts
  private static String fetchValidLine(String script) {
    StringBuilder validLine = new StringBuilder();
    Scanner input = new Scanner(script.toString());
    while (input.hasNextLine()) {
      String line = input.nextLine();
      if (line.startsWith("--") || line.startsWith("\\\\")
	  || line.startsWith("//") || line.startsWith("/*")
	  || line.startsWith("#") || line.trim().isEmpty())
	continue;
      else {
	validLine.append(line.toUpperCase());
      }
    }
    return validLine.toString();
  }

}
