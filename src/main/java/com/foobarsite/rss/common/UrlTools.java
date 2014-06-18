package com.foobarsite.rss.common;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @see http://en.wikipedia.org/wiki/URI_scheme
 */
public class UrlTools {

	public static final String PROTOCOL_REGEXP = "http://|https://";

	/**
	 * http://stackoverflow.com/questions/2230676/how-to-check-for-a-valid-url-in-java
	 */
	public static boolean isValidUrl(String url) {
		boolean valid = false;
		if (!StringTools.isBlankOrNull(url)) {
			try {
				URL u = new URL(url); // check for the protocol
				u.toURI(); // does the extra checking required for validation of URI
				valid = true;
			} catch (URISyntaxException | MalformedURLException ignore) {
				// ignored on purpose
			}
		}
		return valid;
	}

	public static String trimUrl(String url) {
		if (StringTools.isBlankOrNull(url)) {
			return url;
		} else {
			String trimmed = url.trim();
			return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length()-1) : trimmed;
		}
	}

	public static String getBaseUrl(String url) throws MalformedURLException {
		return getBaseUrl(new URL(url));
	}

	public static String getBaseUrl(URL url) {
		StringBuilder builder = new StringBuilder();
		if (url != null && !url.toString().isEmpty()) {
			builder.append(url.getProtocol());
			builder.append("://");
			builder.append(url.getHost());
			if (url.getPort() > 0) {
				builder.append(":").append(url.getPort());
			}
		}
		return builder.toString();
	}

	public static String getHost(String url) throws MalformedURLException {
		return getHost(new URL(url));
	}

	public static String getHost(URL url) {
		StringBuilder builder = new StringBuilder();
		if (url != null && !url.toString().isEmpty()) {
			builder.append(url.getHost());
			if (url.getPort() > 0) {
				builder.append(":").append(url.getPort());
			}
			builder.append(url.getPath());
			if (url.getQuery() != null) {
				builder.append("?").append(url.getQuery());
			}
			if (url.getRef() != null) {
				builder.append("#").append(url.getRef());
			}
		}
		return builder.toString();
	}

	public static String stripProtocol(String url) {
		return url.replaceFirst(PROTOCOL_REGEXP, "");
	}

}
