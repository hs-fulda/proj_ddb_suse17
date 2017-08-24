package fdbs;

public class UnicodeManager {
	public static String replaceUnicodesWithChars(String query) {
		if (query.contains(getUnicodeFromCharForReplacing("Ä"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("Ä"), "Ä");
		}
		if (query.contains(getUnicodeFromCharForReplacing("Ü"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("Ü"), "Ü");
		}
		if (query.contains(getUnicodeFromCharForReplacing("Ö"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("Ö"), "Ö");
		}
		if (query.contains(getUnicodeFromCharForReplacing("ä"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("ä"), "ä");
		}
		if (query.contains(getUnicodeFromCharForReplacing("ü"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("ü"), "ü");
		}
		if (query.contains(getUnicodeFromCharForReplacing("ö"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("ö"), "ö");
		}
		if (query.contains(getUnicodeFromCharForReplacing("ß"))) {
			query = query.replaceAll(getUnicodeFromCharForReplacing("ß"), "ß");
		}

		return query;
	}

	public static String getUnicodedQuery(String query) {
		if (query.contains("Ä")) {
			query = query.replaceAll("Ä", getUnicodeFromChar("Ä"));
		}
		if (query.contains("Ü")) {
			query = query.replaceAll("Ü", getUnicodeFromChar("Ü"));
		}
		if (query.contains("Ö")) {
			query = query.replaceAll("Ö", getUnicodeFromChar("Ö"));
		}
		if (query.contains("ä")) {
			query = query.replaceAll("ä", getUnicodeFromChar("ä"));
		}
		if (query.contains("ü")) {
			query = query.replaceAll("ü", getUnicodeFromChar("ü"));
		}
		if (query.contains("ö")) {
			query = query.replaceAll("ö", getUnicodeFromChar("ö"));
		}
		if (query.contains("ß")) {
			query = query.replaceAll("ß", getUnicodeFromChar("ß"));
		}

		return query;
	}

	private static String getUnicodeFromChar(String c) {
		return "\\u00" + Integer.toHexString(c.toCharArray()[0]);
	}

	private static String getUnicodeFromCharForReplacing(String c) {
		return "u00" + Integer.toHexString(c.toCharArray()[0]);
	}

}
