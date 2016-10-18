package com.ery.server.util;

import java.util.Calendar;
import java.util.Date;

/**
 * 系统默认提供的宏变量; N_YYYY 当前时间-年(四位) N_YY 当前时间-年(后两位) N_MM 当前时间-月(两位) N_M 当前时间-月(当第一位为0时，只取第二位，否则取两位) N_DD 当前时间-天(两位)"); N_D
 * 当前时间-天(当第一位为0时，只取第二位，否则取两位) N_HH 当前时间-小时(两位) N_MI 当前时间-分钟(两位) N_SS 当前时间-秒(两位) N_YYYYMMDD 当前时间-年月日(八位) N_YYYYMM 当前时间-年月(六位)"); N_YYYYMMP
 * 当前时间的前一个月-年月(六位) N_YYYYMMDDN 当前时间的后一天-年月日(八位) N_YYYYMMN 当前时间的下一个月-年月(六位) N_YYYYMMDDP 当前时间的前一天-年月日(八位)
 * 
 * @author wanghao
 * 
 */
public class MacroVariable {

	public static final String DATE_FORMAT_TYPE = "yyyyMMddHHmmss";

	public static String getVarValue(String var) {
		if ("N_YYYY".equals(var)) {
			return getN_YYYY();
		} else if ("N_YY".equals(var)) {
			return getN_YY();
		} else if ("N_MM".equals(var)) {
			return getN_MM();
		} else if ("N_M".equals(var)) {
			return getN_M();
		} else if ("N_DD".equals(var)) {
			return getN_DD();
		} else if ("N_D".equals(var)) {
			return getN_D();
		} else if ("N_HH".equals(var)) {
			return getN_HH();
		} else if ("N_MI".equals(var)) {
			return getN_MI();
		} else if ("N_SS".equals(var)) {
			return getN_SS();
		} else if ("N_YYYYMMDD".equals(var)) {
			return getN_YYYYMMDD();
		} else if ("N_YYYYMM".equals(var)) {
			return getN_YYYYMM();
		} else if ("N_YYYYMMP".equals(var)) {
			return getN_YYYYMMP();
		} else if ("N_YYYYMMDDN".equals(var)) {
			return getN_YYYYMMDDN();
		} else if ("N_YYYYMMN".equals(var)) {
			return getN_YYYYMMN();
		} else if ("N_YYYYMMDDP".equals(var)) {
			return getN_YYYYMMDDP();
		}

		return null;
	}

	// 获取当前时间
	public static String getCurrentN_DATE() {
		Date date = new Date();
		return StringUtils.dateToString(date, DATE_FORMAT_TYPE);
	}

	// 得到前一天
	public static String getBeforeP_DATE() {
		Date date = new Date();
		Calendar cDate = Calendar.getInstance();
		cDate.setTime(date);
		cDate.add(Calendar.DATE, -1); // 得到前一天
		String p_date = StringUtils.dateToString(cDate.getTime(), DATE_FORMAT_TYPE);
		return p_date;
	}

	// 得到后一天
	public static String getAfterNEXT_DATE() {
		Date date = new Date();
		Calendar cNDate = Calendar.getInstance();
		cNDate.setTime(date);
		int day = cNDate.get(Calendar.DATE);
		cNDate.set(Calendar.DATE, day + 1); // 得到后一天
		String next_date = StringUtils.dateToString(cNDate.getTime(), DATE_FORMAT_TYPE);
		return next_date;
	}

	// 得到前一个月
	public static String getAfterP_MONTH() {
		Date date = new Date();
		Calendar cMonth = Calendar.getInstance();
		cMonth.setTime(date);
		cMonth.add(Calendar.MONTH, -1); // 得到前一个月
		String p_month = StringUtils.dateToString(cMonth.getTime(), DATE_FORMAT_TYPE);
		return p_month;
	}

	// 得到后一个月
	public static String getAfterNEXT_MONTH() {
		Date date = new Date();
		Calendar cNMonth = Calendar.getInstance();
		cNMonth.setTime(date);
		int month = cNMonth.get(Calendar.MONTH);
		cNMonth.set(Calendar.MONTH, month + 1); // 得到后一个月
		String next_month = StringUtils.dateToString(cNMonth.getTime(), DATE_FORMAT_TYPE);
		return next_month;
	}

	// 当前时间-年(四位)");
	public static String getN_YYYY() {
		return getCurrentN_DATE().substring(0, 4);
	}

	// 当前时间-年(后两位)");
	public static String getN_YY() {
		return getCurrentN_DATE().substring(2, 4);
	}

	// 当前时间-月(两位)");
	public static String getN_MM() {
		return getCurrentN_DATE().substring(4, 6);
	}

	// 当前时间-月(当第一位为0时，只取第二位，否则取两位)");
	public static String getN_M() {
		return getCurrentN_DATE().substring(4, 6);
	}

	// 当前时间-天(两位)");
	public static String getN_DD() {
		return getCurrentN_DATE().substring(6, 8);
	}

	// 当前时间-天(当第一位为0时，只取第二位，否则取两位)");
	public static String getN_D() {
		return getCurrentN_DATE().substring(6, 8);
	}

	// 当前时间-小时(两位)");
	public static String getN_HH() {
		return getCurrentN_DATE().substring(8, 10);
	}

	// 当前时间-分钟(两位)");
	public static String getN_MI() {
		return getCurrentN_DATE().substring(10, 12);
	}

	// 当前时间-秒(两位)");
	public static String getN_SS() {
		return getCurrentN_DATE().substring(12, 14);
	}

	// 当前时间-年月日(八位)");
	public static String getN_YYYYMMDD() {
		return getCurrentN_DATE().substring(0, 8);
	}

	// 当前时间-年月(六位)");
	public static String getN_YYYYMM() {
		return getCurrentN_DATE().substring(0, 6);
	}

	// 当前时间的前一个月-年月(六位)");
	public static String getN_YYYYMMP() {
		return getAfterP_MONTH().substring(0, 6);
	}

	// 当前时间的后一天-年月日(八位)");
	public static String getN_YYYYMMDDN() {
		return getAfterNEXT_DATE().substring(0, 8);
	}

	// 当前时间的下一个月-年月(六位)");
	public static String getN_YYYYMMN() {
		return getAfterNEXT_MONTH().substring(0, 6);
	}

	// 当前时间的前一天-年月日(八位)");
	public static String getN_YYYYMMDDP() {
		return getBeforeP_DATE().substring(0, 8);
	}
}
