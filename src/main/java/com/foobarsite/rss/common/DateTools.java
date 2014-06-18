package com.foobarsite.rss.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTools {
	private static ThreadLocal<SimpleDateFormat> formatLong = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
		}
	};

	private  static ThreadLocal<SimpleDateFormat> formatShort = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("dd.MM.yyyy");
		}
	};

	/** 631148461000 or 631148461002 */
	public static final Date DEFAULT_DATE_MIN;

	/** 4102441261000 */
	public static final Date DEFAULT_DATE_MAX;

	static {
		Calendar cal = Calendar.getInstance();
		cal.set(1990, 0, 1, 1, 1, 1);
		cal.set(Calendar.MILLISECOND, 0);
		DEFAULT_DATE_MIN = cal.getTime();
		cal.set(2100, 0, 1, 1, 1, 1);
		DEFAULT_DATE_MAX = cal.getTime();
	}

	public static String formatLongDate(Date date) {
		return formatLong.get().format(date);
	}

	public static String formatShortDate(Date date) {
		return formatShort.get().format(date);
	}

	public static boolean equalsIgnoreMinsSecs(Date d1, Date d2) {
		return zeroOutMinsSecs(d1).equals(zeroOutMinsSecs(d2));
	}

	public static Date zeroOutMinsSecs(Date d) {
		if (d == null) {
			return DEFAULT_DATE_MAX;
		}
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		cal.set(Calendar.MILLISECOND, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		return cal.getTime();
	}

	public static boolean isFirstAfterSecond(Date d1, Date d2) {
		if (d1 == null) {
			return false;
		}
		if (d2 == null) {
			return true;
		}
		return d1.after(d2);
	}

}
