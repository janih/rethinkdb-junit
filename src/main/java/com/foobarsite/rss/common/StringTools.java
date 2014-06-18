package com.foobarsite.rss.common;

public class StringTools {

	public static String nullToEmpty(Object obj) {
		return obj == null ? "" : obj.toString();
	}

	public static boolean isBlankOrNull(String str) {
		return str == null ? true : str.isEmpty();
	}

	public static String trim(String str) {
		return isBlankOrNull(str) ? str : str.trim();
	}

	public static String cutToLength(String str, int length) {
		String dots = "...";
		if (isBlankOrNull(str)) {
			return "";
		}
		else if (str.length() <= dots.length()) {
			return str;
		}
		else {
			return str.length() >= length ? (str.substring(0, length - dots.length()) + dots) : str;
		}
	}

	public static boolean areEquals(String str1, String str2) {
		if (str1 == str2 || str1 == null && str2 == null) {
			return true;
		}
		else if (str1 != null && str2 == null || str1 == null && str2 != null) {
			return false;
		}
		else {
			return str1.equals(str2);
		}
	}

	public static boolean areEqualsAlsoNulls(String str1, String str2) {
		if (str1 == str2 || str1 == null || str2 == null) {
			return true;
		}
		else {
			return str1.equals(str2);
		}
	}
}
