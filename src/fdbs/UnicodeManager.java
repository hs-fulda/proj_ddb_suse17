package fdbs;

public class UnicodeManager {
	public static String replaceUnicodesWithChars(String query) {
		if (query.contains(getUnicodeFromCharForReplacing("�"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
		}
		if (query.contains(getUnicodeFromCharForReplacing("�"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
		}
		if (query.contains(getUnicodeFromCharForReplacing("�"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
		}
		if (query.contains(getUnicodeFromCharForReplacing("�"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
		}
		if (query.contains(getUnicodeFromCharForReplacing("�"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
		}
		if (query.contains(getUnicodeFromCharForReplacing("�"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
		}
		if (query.contains(getUnicodeFromCharForReplacing("�"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
		}

		return query;
	}

	public static String getUnicodedQuery(String query) {
		if (query.contains("�")) {
			query = query.replaceAll("�", getUnicodeFromChar("�"));
		}
		if (query.contains("�")) {
			query = query.replaceAll("�", getUnicodeFromChar("�"));
		}
		if (query.contains("�")) {
			query = query.replaceAll("�", getUnicodeFromChar("�"));
		}
		if (query.contains("�")) {
			query = query.replaceAll("�", getUnicodeFromChar("�"));
		}
		if (query.contains("�")) {
			query = query.replaceAll("�", getUnicodeFromChar("�"));
		}
		if (query.contains("�")) {
			query = query.replaceAll("�", getUnicodeFromChar("�"));
		}
		if (query.contains("�")) {
			query = query.replaceAll("�", getUnicodeFromChar("�"));
		}

		return query;
	}

	private static String getUnicodeFromChar(String c) {
		return "\\u00" + Integer.toHexString(c.toCharArray()[0]);
	}
	
	private static String getUnicodeFromCharForReplacing(String c) {
		return "u00" + Integer.toHexString(c.toCharArray()[0]);
	}

	public static char getCharFromUnicode(String unicode) {
		Integer code = Integer.parseInt(unicode.substring(2), 16); // the
																	// integer
																	// 65 in
																	// base 10
		return Character.toChars(code)[0];
	}

}
