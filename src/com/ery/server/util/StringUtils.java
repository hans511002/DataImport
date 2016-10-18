package com.ery.server.util;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * String类型的工具类
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-14
 * @version v1.0
 */
public class StringUtils {
	public static final String DATE_FORMAT_TYPE1 = "yyyy-MM-dd HH:mm:ss";
	public static final String DATE_FORMAT_TYPE2 = "yyyyMMddHHmmss";
	public static final String DATE_FORMAT_TYPE3 = "yyyy-MM-dd";
	public static final String DOUBLE_FORMAT_pattern1 = "0";
	public static final String DOUBLE_FORMAT_pattern2 = "0.0";
	public static final String DOUBLE_FORMAT_pattern3 = "0.00";
	public static final String DOUBLE_FORMAT_pattern4 = "#,##0";
	public static final String DOUBLE_FORMAT_pattern5 = "#,##0.0";
	public static final String DOUBLE_FORMAT_pattern6 = "#,##0.00";
	public static final String DOUBLE_FORMAT_pattern7 = "^[0-9]+$";

	/**
	 * String类型和数字类型比较
	 * 
	 * @param obj1
	 *            对象1
	 * @param obj2
	 *            对象2
	 * @return 返回最大值
	 */
	public static final Object getBigObject(Object obj1, Object obj2) {
		if (null == obj1 && null == obj2) {
			return null;
		}

		if (null != obj1 && null == obj2) {
			return obj1;
		}

		if (null == obj1 && null != obj2) {
			return obj2;
		}

		try {
			if (Double.parseDouble((String) obj1.toString()) > Double.parseDouble((String) obj2.toString())) {
				return obj1;
			} else {
				return obj2;
			}
		} catch (Exception e) {
		}

		// string比较
		if (obj1.toString().compareTo(obj2.toString()) > 0) {
			return obj1;
		} else {
			return obj2;
		}
	}

	/**
	 * String类型和数字类型比较：返回最小值
	 * 
	 * @param obj1
	 *            对象1
	 * @param obj2
	 *            对象2
	 * @return 返回最大值
	 */
	public static final Object getSmallObject(Object obj1, Object obj2) {
		if (null == obj1 && null == obj2) {
			return null;
		}

		if (null != obj1 && null == obj2) {
			return obj1;
		}

		if (null == obj1 && null != obj2) {
			return obj2;
		}

		try {
			if (Double.parseDouble((String) obj1.toString()) > Double.parseDouble((String) obj2.toString())) {
				return obj2;
			} else {
				return obj1;
			}
		} catch (Exception e) {
		}

		// string比较
		if (obj1.toString().compareTo(obj2.toString()) > 0) {
			return obj2;
		} else {
			return obj1;
		}
	}

	/**
	 * 将String转为int
	 * 
	 * @param obj
	 *            对象
	 * @return int
	 */
	public static final int objectToInt(Object obj) {
		if (null == obj) {
			return 0;
		}
		try {
			if (obj instanceof String) {
				return Integer.parseInt((String) obj);
			} else if (obj instanceof Integer) {
				return (Integer) obj;
			}
		} catch (Exception e) {
			return 0;
		}
		return 0;
	}

	/**
	 * 将String转为int
	 * 
	 * @param obj
	 *            对象
	 * @return int
	 */
	public static final String objectToString(Object obj, String defaultvalue) {
		if (null == obj) {
			return defaultvalue;
		}
		try {
			return obj.toString();
		} catch (Exception e) {
			return defaultvalue;
		}
	}

	/**
	 * 将String转为double
	 * 
	 * @param obj
	 *            对象
	 * @return double
	 */
	public static final double stringToDouble(Object obj) {
		try {
			if (obj instanceof String) {
				return Double.parseDouble((String) obj);
			}
		} catch (Exception e) {
			return 0.0;
		}
		return 0.0;
	}

	/**
	 * 将String转为double
	 * 
	 * @param obj
	 *            对象
	 * @return double
	 */
	public static final double stringToDouble(Object obj, int defaultValue) {
		try {
			if (obj instanceof String) {
				return Double.parseDouble((String) obj);
			}
		} catch (Exception e) {
			return defaultValue;
		}
		return defaultValue;
	}

	/**
	 * 将String转为int
	 * 
	 * @param obj
	 *            对象
	 * @param defaultValue
	 *            默认值
	 * @return int
	 */
	public static final int stringToInt(String obj, int defaultValue) {
		try {
			return Integer.parseInt(obj);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	/**
	 * 获取时分秒的long型
	 * 
	 * @param mintime
	 *            时间(毫秒)
	 * @return 时分秒的long型
	 * @throws ParseException
	 *             异常
	 */
	static SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static long getTime(long mintime) throws ParseException {
		Date nowTime = new Date(mintime);
		String time = sdFormatter.format(nowTime);
		long tims = sdFormatter.parse(time).getTime();
		return tims / 1000;
	}

	/**
	 * 获取月份(yyyyMM)
	 * 
	 * @param date
	 *            日期
	 * @return 月份
	 * @throws ParseException
	 */
	public static String getMonthNo(Date date) throws ParseException {
		SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyyMM");
		return sdFormatter.format(date);
	}

	/**
	 * 获取天(yyyyMMdd)
	 * 
	 * @param date
	 *            日期
	 * @return 天
	 * @throws ParseException
	 *             异常
	 */
	public static String getDateNo(Date date) throws ParseException {
		SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyyMMdd");
		return sdFormatter.format(date);
	}

	/**
	 * 将double转为自定格式的字符串
	 * 
	 * @param d
	 *            double
	 * @return string
	 */
	public static final String doubleToString(Double d, String format) {
		DecimalFormat df = new DecimalFormat(format);
		return df.format(d);
	}

	/**
	 * date类型转换为String类型
	 * 
	 * @param data
	 *            Date类型的时间
	 * @param formatType
	 *            formatType格式为yyyy-MM-dd HH:mm:ss//yyyy年MM月dd日 HH时mm分ss秒
	 * @return String
	 */
	public static String dateToString(Date data, String formatType) {
		if (null == data) {
			return "";
		}
		return new SimpleDateFormat(formatType).format(data);
	}

	/**
	 * ong类型转换为String类型
	 * 
	 * @param currentTime
	 *            要转换的long类型的时间
	 * @param formatType
	 *            要转换的string类型的时间格式
	 * @return String
	 * @throws ParseException
	 *             异常
	 */
	public static String longToString(long currentTime, String formatType) throws ParseException {
		Date dateOld = new Date(currentTime); // 根据long类型的毫秒数生命一个date类型的时间
		String sDateTime = dateToString(dateOld, formatType); // 把date类型的时间转换为string
		return sDateTime;
	}

	public static String stringToStrDate(String strTime) {
		Pattern pattern = Pattern.compile(DOUBLE_FORMAT_pattern7);
		// 检查是否为数字
		if (!matcher(strTime, pattern)) {
			return null;
		}

		// 检查是否满足长度
		if (strTime.length() > 14) {
			return null;
		}

		// 拼接长度满足14位
		if (strTime.length() < 14) {
			int len = 14 - strTime.length();
			for (int i = 0; i < len; i++) {
				strTime += 0;
			}
		}

		try {
			Date date = stringToDate(strTime, "yyyyMMddHHmmss");
			return dateToString(date, DATE_FORMAT_TYPE1);
		} catch (ParseException e) {
			return null;
		}
	}

	/**
	 * string类型转换为date类型
	 * 
	 * @param strTime要转换的string类型的时间
	 *            ，
	 * @param formatType要转换的格式yyyy
	 *            -MM-dd HH:mm:ss//yyyy年MM月dd日 HH时mm分ss秒，
	 *            strTime的时间格式必须要与formatType的时间格式相同
	 * @return date
	 * @throws ParseException
	 */
	public static Date stringToDate(String strTime, String formatType) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat(formatType);
		Date date = null;
		date = formatter.parse(strTime);
		return date;
	}

	/**
	 * long转换为Date类型
	 * 
	 * @param currentTime要转换的long类型的时间
	 * @param formatType要转换的时间格式yyyy
	 *            -MM-dd HH:mm:ss//yyyy年MM月dd日 HH时mm分ss秒
	 * @return date
	 * @throws ParseException
	 *             异常
	 */
	public static Date longToDate(long currentTime, String formatType) throws ParseException {
		Date dateOld = new Date(currentTime); // 根据long类型的毫秒数生命一个date类型的时间
		String sDateTime = dateToString(dateOld, formatType); // 把date类型的时间转换为string
		Date date = stringToDate(sDateTime, formatType); // 把String类型转换为Date类型
		return date;
	}

	/**
	 * string类型转换为long类型
	 * 
	 * @param strTime要转换的String类型的时间
	 * @param formatType时间格式
	 *            ,strTime的时间格式和formatType的时间格式必须相同
	 * @return long
	 * @throws ParseException
	 *             异常
	 */
	public static long stringToLong(String strTime, String formatType) throws ParseException {
		Date date = stringToDate(strTime, formatType); // String类型转成date类型
		if (date == null) {
			return 0;
		} else {
			long currentTime = dateToLong(date); // date类型转成long类型
			return currentTime;
		}
	}

	// date类型转换为long类型
	// date要转换的date类型的时间
	public static long dateToLong(Date date) {
		return date.getTime();
	}

	/**
	 * 转义正则表达则特殊字符
	 * 
	 * @param splitChar
	 *            字符
	 * @return 特殊字符
	 */
	public static String parseSpecialChar(String splitChar) {
		String res = splitChar;
		if (res.indexOf("|") >= 0) {
			res = res.replaceAll("\\|", "\\|");
		}
		if (res.indexOf("~") >= 0) {
			res = res.replaceAll("\\~", "\\~");
		}
		if (res.indexOf("^") >= 0) {
			res = res.replaceAll("\\^", "\\^");
		}
		if (res.indexOf("[") >= 0) {
			res = res.replaceAll("\\[", "\\[");
		}
		if (res.indexOf("]") >= 0) {
			res = res.replaceAll("\\]", "\\]");
		}
		if (res.indexOf("{") >= 0) {
			res = res.replaceAll("\\{", "\\{");
		}
		if (res.indexOf("}") >= 0) {
			res = res.replaceAll("\\}", "\\}");
		}
		if (res.indexOf("(") >= 0) {
			res = res.replaceAll("\\(", "\\(");
		}
		if (res.indexOf(")") >= 0) {
			res = res.replaceAll("\\)", "\\)");
		}
		return res;
	}

	/**
	 * 将long数组转为字符串数组
	 * 
	 * @param values
	 *            long数组
	 * @return 字符串数组
	 */
	public static String[] valueOfLongToString(long[] values) {
		String reValue[] = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			reValue[i] = String.valueOf(values[i]);
		}

		return reValue;
	}

	/**
	 * 将字符串数组转为long数组
	 * 
	 * @param values
	 *            字符串数组
	 * @return long数组
	 */
	public static long[] valueOfStringToLong(String[] values) {
		if (null == values) {
			return new long[0];
		}

		long reValue[] = new long[values.length];
		for (int i = 0; i < values.length; i++) {
			try {
				reValue[i] = Long.parseLong(values[i]);
			} catch (Exception e) {
				reValue[i] = -1;
			}
		}

		return reValue;
	}

	/**
	 * 将String数组转为为hashmap 例如： a1:12,c1:32,f3:dd 转为 [key=a1,value=12
	 * key=c1,value=32 key=f3,value=dd]
	 * 
	 * @param values
	 *            字符串
	 * @param perSig
	 *            分隔符
	 * @param subSig
	 *            子分隔符
	 * @return 分割后的列表
	 */
	public static Map<String, String> valueOfStringToHashMap(String value, String perSig, String subSig) {
		if (null == value || null == perSig || null == subSig) {
			return null;
		}

		Map<String, String> hashMap = new HashMap<String, String>();
		String keyValue[] = value.split(perSig);
		for (int i = 0; i < keyValue.length; i++) {
			String kv[] = keyValue[i].split(subSig);
			if (kv.length == 2 && null != kv[0] && null != kv[1]) {
				hashMap.put(kv[0], kv[1]);
			}
		}

		return hashMap;
	}

	/**
	 * 拼接字符串数组
	 * 
	 * @param content
	 *            字符串数组
	 * @param sign
	 *            分隔符
	 * @return 拼接后的字符串
	 */
	public static String toString(String[] content, String sign) {
		if (null == content) {
			return null;
		}

		sign = null == sign ? "," : sign;
		StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < content.length; i++) {
			strBuilder.append(content[i]);
			if (i < content.length - 1) {
				strBuilder.append(sign);
			}
		}

		return strBuilder.toString();
	}

	/**
	 * 获取异常日志内容
	 * 
	 * @param e
	 *            异常
	 * @return 异常内容
	 */
	public static String printStackTrace(Throwable e) {
		StringWriter stm = new StringWriter();
		PrintWriter wrt = new PrintWriter(stm);
		e.printStackTrace(wrt);
		wrt.close();
		return stm.toString();
	}

	/**
	 * 截取查询sql语句 例如: select * from t_user where a=1; 返回'select * from'
	 * 
	 * @param sql
	 *            sql语句
	 * @return 截取的语句
	 */
	public static String subSqlSelectToFrom(String sql) {
		if (null == sql || sql.trim().length() <= 0) {
			return null;
		}

		String tmp = sql.trim().toUpperCase();
		int selectIndex = tmp.indexOf("SELECT");
		int fromIndex = tmp.indexOf("FROM");
		if (selectIndex <= -1 || fromIndex <= 7) {
			return sql = null;
		} else {
			return sql = sql.substring(selectIndex, fromIndex + 4);
		}
	}

	/**
	 * 将Object[]转换为String
	 * 
	 * @param lstContent
	 *            字符串列表
	 * @param sign
	 *            分隔符
	 * @return 拼接后的字符串
	 */
	public static LinkedList<Object> parseArrayToList(Object[] lstContent) {
		LinkedList<Object> lst = new LinkedList<Object>();
		for (int i = 0; i < lstContent.length; i++) {
			Object obj = lstContent[i];
			lst.add(obj);
		}

		return lst;
	}

	/**
	 * 将Object[]转换为String
	 * 
	 * @param lstContent
	 *            字符串列表
	 * @param sign
	 *            分隔符
	 * @return 拼接后的字符串
	 */
	public static String parseArrayToString(Object[] lstContent, String sign) {
		StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < lstContent.length; i++) {
			Object obj = lstContent[i];
			if (null == obj) {
				strBuilder.append("null");
			} else {
				strBuilder.append(obj.toString());
			}

			if (i != lstContent.length - 1) {
				strBuilder.append(sign);
			}
		}

		return strBuilder.toString();
	}

	/**
	 * 将list转换为String
	 * 
	 * @param lstContent
	 *            字符串列表
	 * @param sign
	 *            分隔符
	 * @return 拼接后的字符串
	 */
	public static String parseListToString(List<?> lstContent, String sign) {
		StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < lstContent.size(); i++) {
			Object obj = lstContent.get(i);
			if (null == obj) {
				strBuilder.append("null");
			} else {
				strBuilder.append(obj.toString());
			}

			if (i != lstContent.size() - 1) {
				strBuilder.append(sign);
			}
		}

		return strBuilder.toString();
	}

	/**
	 * 将list转换为String
	 * 
	 * @param lstContent
	 *            字符串列表
	 * @param sign
	 *            分隔符
	 * @return 拼接后的字符串
	 */
	public static String parseSetToString(Set<?> lstContent, String sign) {
		StringBuilder strBuilder = new StringBuilder();
		for (Object obj : lstContent) {
			if (null == obj) {
				strBuilder.append("null");
			} else {
				strBuilder.append(obj.toString());
			}
			strBuilder.append(sign);
		}

		return strBuilder.toString();
	}

	/**
	 * 将String数组转为Set集合
	 * 
	 * @param value
	 *            String数组
	 * @return set集合
	 */
	public static Set<String> parseStringArrayToSet(String[] value) {
		Set<String> set = new HashSet<String>();
		Collections.addAll(set, value);
		return set;
	}

	/**
	 * 将空格( )替换为下划线(_)
	 * 
	 * @param value
	 *            字符串
	 * @return 替换后的字符串
	 */
	public static String changeBlankToUnderline(String value) {
		if (null == value || value.trim().length() <= 0) {
			return "";
		}

		return value.replaceAll(" ", "_");
	}

	/**
	 * 获取唯一号码
	 * 
	 * @return uuid的前10位拼接上当前时间的毫秒数，再加密后的取前10位
	 * @throws NoSuchAlgorithmException
	 *             异常
	 */
	public static String getUniqueId() {
		UUID uuid = UUID.randomUUID();
		MessageDigest md5;
		try {
			md5 = MessageDigest.getInstance("MD5");
			md5.update((uuid.toString() + System.currentTimeMillis()).getBytes());
			return byte2hex(md5.digest()).substring(0, 10);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return uuid.toString().substring(0, 10);
	}

	/**
	 * 字节数组转为字符串
	 * 
	 * @param b
	 *            字节数组
	 * @return 字符串
	 */
	private static String byte2hex(byte[] b) // 二行制转字符串
	{
		String hs = "";
		String stmp = "";
		for (int n = 0; n < b.length; n++) {
			stmp = (java.lang.Integer.toHexString(b[n] & 0XFF));
			if (stmp.length() == 1) {
				hs = hs + "0" + stmp;
			} else {
				hs = hs + stmp;
			}
		}
		return hs.toUpperCase();
	}

	/**
	 * 将String数组转为大写
	 * 
	 * @param value
	 * @return
	 */
	public static String[] changeStringArrayToUpper(String[] value) {
		if (null == value) {
			return null;
		}
		String[] tmp = new String[value.length];
		for (int i = 0; i < value.length; i++) {
			if (null == value[i]) {
				tmp[i] = null;
				continue;
			}
			tmp[i] = value[i].toUpperCase();
		}

		return tmp;
	}

	public static boolean matcher(String value, Pattern pattern) {
		if (null == pattern) {
			return true;
		}

		return pattern.matcher(value).matches();
	}

	/**
	 * 后缀为斜杠的目录
	 * 
	 * @param path
	 * @return
	 */
	public static String getFileName(String path) {
		if (null == path || path.trim().length() <= 0) {
			return null;
		}
		int flag = path.lastIndexOf("/");
		if (flag != -1) {
			return path.substring(flag + 1, path.length());
		} else {
			return "";
		}
	}

	/**
	 * 返回parent路径，后缀没有"/"
	 * 
	 * @param path
	 * @return
	 */
	public static String getFilePathParentPath(String path) {
		if (null == path || path.trim().length() <= 0) {
			return null;
		}

		String paths[] = path.split("/");
		if (paths.length == 0) {
			return null;
		}

		if (path.startsWith("/")) {
			if (paths.length == 2) {
				return "/";
			} else if (paths.length > 2) {
				StringBuilder strBuid = new StringBuilder();
				strBuid.append("/");
				for (int i = 0; i < paths.length - 1; i++) {
					strBuid.append(paths[i]);
					if (i != 0 && i != paths.length - 2) {
						strBuid.append("/");
					}
				}

				return strBuid.toString();
			}
		} else {
			if (paths.length == 1) {
				return null;
			} else if (paths.length == 2) {
				return paths[0];
			} else if (paths.length > 2) {
				StringBuilder strBuid = new StringBuilder();
				for (int i = 0; i < paths.length - 1; i++) {
					strBuid.append(paths[i]);
					if (i != paths.length - 1) {
						strBuid.append("/");
					}
				}

				return strBuid.toString();
			}
		}

		return null;
	}

	/**
	 * 后缀为斜杠的目录
	 * 
	 * @param path
	 * @return
	 */
	public static String getSlashSuffixPath(String path) {
		if (null == path || path.trim().length() <= 0) {
			return null;
		}
		if (path.endsWith("/")) {
			return path;
		} else {
			return path + "/";
		}
	}

	public static String decodeString(String value, String beginSign, String endSign) {
		if (null == value || null == beginSign || null == endSign) {
			return null;
		}

		if (value.trim().length() < 2 && beginSign.trim().length() <= 0 && endSign.trim().length() <= 0) {
			return null;
		}

		if (!value.startsWith(beginSign) || !value.endsWith(endSign)) {
			return null;
		}

		return value.substring(1, value.length() - 1);
	}

	/**
	 * 
	 * @param path
	 * @param id
	 * @return
	 */
	public static String getFileOnlyKey(String path, int id) {
		return id + DataSHAUtil.md5Encrypt(id + path);
	}

	/**
	 * 
	 * @param outputHBaseOutColumnRelation
	 *            格式：[a:defvaluea],[b:defvalueb]
	 * @return
	 */

	public static Map<String, String> decodeOutColumnDefaultValue(String[] outputFieldDefaultValue) {
		Map<String, String> outColumnDefaultValue = new HashMap<String, String>();
		if (null == outputFieldDefaultValue || outputFieldDefaultValue.length <= 0) {
			return outColumnDefaultValue;
		}

		for (int i = 0; i < outputFieldDefaultValue.length; i++) {
			String temp = StringUtils.decodeString(outputFieldDefaultValue[i], "[", "]");
			if (null == temp) {
				continue;
			}

			String rela[] = temp.split(":");
			if (rela.length < 2) {
				continue;
			}

			outColumnDefaultValue.put(rela[0], temp.substring(temp.indexOf(":") + 1));
		}

		return outColumnDefaultValue;
	}

	/**
	 * 解析输出字段与列簇-列的拆分对应关系
	 * 
	 * @param hbaseColumnRelation
	 *            格式：[列簇:列名称:[a,b,c]]-[列簇:列名称:[a,b,c]]-[列簇:列名称:[a,b,c]]
	 * @return
	 */
	public static void decodeOutColumnSplitRelation(String hbaseColumnRelation, List<String[]> list, List<String[]> rela) {
		if (null == hbaseColumnRelation) {
			return;
		}

		String[] str = hbaseColumnRelation.split("-");
		for (int i = 0; i < str.length; i++) {
			String temp = StringUtils.decodeString(str[i], "[", "]");
			if (null == temp) {
				continue;
			}

			String col[] = temp.split(":");
			if (col.length < 3) {
				continue;
			}

			String cluster = col[0];
			String column = col[1];
			String outputField = StringUtils.decodeString(col[2], "[", "]");
			if (null == outputField) {
				continue;
			}

			String field[] = outputField.split(",");
			if (field.length < 0) {
				continue;
			}

			rela.add(field);
			list.add(new String[] { cluster, column });
		}
	}

	public static String[] getNameValue(String tmpParam, String sign) {
		if (null == tmpParam || tmpParam.trim().length() <= 0) {
			return null;
		}

		if (null == sign) {
			return null;
		}

		tmpParam = tmpParam.trim();
		String tmp[] = tmpParam.split(sign);
		if (tmp.length != 2) {
			return null;
		}

		return tmp;
	}

	private static final DecimalFormat decimalFormat;
	static {
		NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ENGLISH);
		decimalFormat = (DecimalFormat) numberFormat;
		decimalFormat.applyPattern("#.##");
	}

	/**
	 * Given a full hostname, return the word upto the first dot.
	 * 
	 * @param fullHostname
	 *            the full hostname
	 * @return the hostname to the first dot
	 */
	public static String simpleHostname(String fullHostname) {
		int offset = fullHostname.indexOf('.');
		if (offset != -1) {
			return fullHostname.substring(0, offset);
		}
		return fullHostname;
	}

	private static DecimalFormat oneDecimal = new DecimalFormat("0.0");

	/**
	 * Given an integer, return a string that is in an approximate, but human
	 * readable format. It uses the bases 'k', 'm', and 'g' for 1024, 1024**2,
	 * and 1024**3.
	 * 
	 * @param number
	 *            the number to format
	 * @return a human readable form of the integer
	 */
	public static String humanReadableInt(long number) {
		long absNumber = Math.abs(number);
		double result = number;
		String suffix = "";
		if (absNumber < 1024) {
			// nothing
		} else if (absNumber < 1024 * 1024) {
			result = number / 1024.0;
			suffix = "k";
		} else if (absNumber < 1024 * 1024 * 1024) {
			result = number / (1024.0 * 1024);
			suffix = "m";
		} else {
			result = number / (1024.0 * 1024 * 1024);
			suffix = "g";
		}
		return oneDecimal.format(result) + suffix;
	}

	/**
	 * Format a percentage for presentation to the user.
	 * 
	 * @param done
	 *            the percentage to format (0.0 to 1.0)
	 * @param digits
	 *            the number of digits past the decimal point
	 * @return a string representation of the percentage
	 */
	public static String formatPercent(double done, int digits) {
		DecimalFormat percentFormat = new DecimalFormat("0.00%");
		double scale = Math.pow(10.0, digits + 2);
		double rounded = Math.floor(done * scale);
		percentFormat.setDecimalSeparatorAlwaysShown(false);
		percentFormat.setMinimumFractionDigits(digits);
		percentFormat.setMaximumFractionDigits(digits);
		return percentFormat.format(rounded / scale);
	}

	/**
	 * Given an array of strings, return a comma-separated list of its elements.
	 * 
	 * @param strs
	 *            Array of strings
	 * @return Empty string if strs.length is 0, comma separated list of strings
	 *         otherwise
	 */

	public static String arrayToString(String[] strs) {
		if (strs.length == 0) {
			return "";
		}
		StringBuffer sbuf = new StringBuffer();
		sbuf.append(strs[0]);
		for (int idx = 1; idx < strs.length; idx++) {
			sbuf.append(",");
			sbuf.append(strs[idx]);
		}
		return sbuf.toString();
	}

	/**
	 * Given an array of bytes it will convert the bytes to a hex string
	 * representation of the bytes
	 * 
	 * @param bytes
	 * @param start
	 *            start index, inclusively
	 * @param end
	 *            end index, exclusively
	 * @return hex string representation of the byte array
	 */
	public static String byteToHexString(byte[] bytes, int start, int end) {
		if (bytes == null) {
			throw new IllegalArgumentException("bytes == null");
		}
		StringBuilder s = new StringBuilder();
		for (int i = start; i < end; i++) {
			s.append(String.format("%02x", bytes[i]));
		}
		return s.toString();
	}

	/** Same as byteToHexString(bytes, 0, bytes.length). */
	public static String byteToHexString(byte bytes[]) {
		return byteToHexString(bytes, 0, bytes.length);
	}

	/**
	 * Given a hexstring this will return the byte array corresponding to the
	 * string
	 * 
	 * @param hex
	 *            the hex String array
	 * @return a byte array that is a hex string representation of the given
	 *         string. The size of the byte array is therefore hex.length/2
	 */
	public static byte[] hexStringToByte(String hex) {
		byte[] bts = new byte[hex.length() / 2];
		for (int i = 0; i < bts.length; i++) {
			bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
		}
		return bts;
	}

	/**
	 * 
	 * @param uris
	 */
	public static String uriToString(URI[] uris) {
		if (uris == null) {
			return null;
		}
		StringBuffer ret = new StringBuffer(uris[0].toString());
		for (int i = 1; i < uris.length; i++) {
			ret.append(",");
			ret.append(uris[i].toString());
		}
		return ret.toString();
	}

	/**
	 * 
	 * @param str
	 */
	public static URI[] stringToURI(String[] str) {
		if (str == null)
			return null;
		URI[] uris = new URI[str.length];
		for (int i = 0; i < str.length; i++) {
			try {
				uris[i] = new URI(str[i]);
			} catch (URISyntaxException ur) {
				System.out.println("Exception in specified URI's " + StringUtils.printStackTrace(ur));
				// making sure its asssigned to null in case of an error
				uris[i] = null;
			}
		}
		return uris;
	}

	/**
	 * 
	 * @param str
	 */
	public static File[] stringToFile(String[] str) {
		if (str == null) {
			return null;
		}
		File[] p = new File[str.length];
		for (int i = 0; i < str.length; i++) {
			p[i] = new File(str[i]);
		}
		return p;
	}

	/**
	 * 
	 * Given a finish and start time in long milliseconds, returns a String in
	 * the format Xhrs, Ymins, Z sec, for the time difference between two times.
	 * If finish time comes before start time then negative valeus of X, Y and Z
	 * wil return.
	 * 
	 * @param finishTime
	 *            finish time
	 * @param startTime
	 *            start time
	 */
	public static String formatTimeDiff(long finishTime, long startTime) {
		long timeDiff = finishTime - startTime;
		return formatTime(timeDiff);
	}

	/**
	 * 
	 * Given the time in long milliseconds, returns a String in the format Xhrs,
	 * Ymins, Z sec.
	 * 
	 * @param timeDiff
	 *            The time difference to format
	 */
	public static String formatTime(long timeDiff) {
		StringBuffer buf = new StringBuffer();
		long hours = timeDiff / (60 * 60 * 1000);
		long rem = (timeDiff % (60 * 60 * 1000));
		long minutes = rem / (60 * 1000);
		rem = rem % (60 * 1000);
		long seconds = rem / 1000;

		if (hours != 0) {
			buf.append(hours);
			buf.append("hrs, ");
		}
		if (minutes != 0) {
			buf.append(minutes);
			buf.append("mins, ");
		}
		// return "0sec if no difference
		buf.append(seconds);
		buf.append("sec");
		return buf.toString();
	}

	/**
	 * Formats time in ms and appends difference (finishTime - startTime) as
	 * returned by formatTimeDiff(). If finish time is 0, empty string is
	 * returned, if start time is 0 then difference is not appended to return
	 * value.
	 * 
	 * @param dateFormat
	 *            date format to use
	 * @param finishTime
	 *            fnish time
	 * @param startTime
	 *            start time
	 * @return formatted value.
	 */
	public static String getFormattedTimeWithDiff(DateFormat dateFormat, long finishTime, long startTime) {
		StringBuffer buf = new StringBuffer();
		if (0 != finishTime) {
			buf.append(dateFormat.format(new Date(finishTime)));
			if (0 != startTime) {
				buf.append(" (" + formatTimeDiff(finishTime, startTime) + ")");
			}
		}
		return buf.toString();
	}

	/**
	 * Returns an arraylist of strings.
	 * 
	 * @param str
	 *            the comma seperated string values
	 * @return the arraylist of the comma seperated string values
	 */
	public static String[] getStrings(String str) {
		Collection<String> values = getStringCollection(str);
		if (values.size() == 0) {
			return null;
		}
		return values.toArray(new String[values.size()]);
	}

	/**
	 * Returns a collection of strings.
	 * 
	 * @param str
	 *            comma seperated string values
	 * @return an <code>ArrayList</code> of string values
	 */
	public static Collection<String> getStringCollection(String str) {
		List<String> values = new ArrayList<String>();
		if (str == null)
			return values;
		StringTokenizer tokenizer = new StringTokenizer(str, ",");
		values = new ArrayList<String>();
		while (tokenizer.hasMoreTokens()) {
			values.add(tokenizer.nextToken());
		}
		return values;
	}

	final public static char COMMA = ',';
	final public static String COMMA_STR = ",";
	final public static char ESCAPE_CHAR = '\\';

	/**
	 * Split a string using the default separator
	 * 
	 * @param str
	 *            a string that may have escaped separator
	 * @return an array of strings
	 */
	public static String[] split(String str) {
		return split(str, ESCAPE_CHAR, COMMA);
	}

	/**
	 * Split a string using the given separator
	 * 
	 * @param str
	 *            a string that may have escaped separator
	 * @param escapeChar
	 *            a char that be used to escape the separator
	 * @param separator
	 *            a separator char
	 * @return an array of strings
	 */
	public static String[] split(String str, char escapeChar, char separator) {
		if (str == null) {
			return null;
		}
		ArrayList<String> strList = new ArrayList<String>();
		StringBuilder split = new StringBuilder();
		int index = 0;
		while ((index = findNext(str, separator, escapeChar, index, split)) >= 0) {
			++index; // move over the separator for next search
			strList.add(split.toString());
			split.setLength(0); // reset the buffer
		}
		strList.add(split.toString());
		// remove trailing empty split(s)
		int last = strList.size(); // last split
		while (--last >= 0 && "".equals(strList.get(last))) {
			strList.remove(last);
		}
		return strList.toArray(new String[strList.size()]);
	}

	/**
	 * Finds the first occurrence of the separator character ignoring the
	 * escaped separators starting from the index. Note the substring between
	 * the index and the position of the separator is passed.
	 * 
	 * @param str
	 *            the source string
	 * @param separator
	 *            the character to find
	 * @param escapeChar
	 *            character used to escape
	 * @param start
	 *            from where to search
	 * @param split
	 *            used to pass back the extracted string
	 */
	public static int findNext(String str, char separator, char escapeChar, int start, StringBuilder split) {
		int numPreEscapes = 0;
		for (int i = start; i < str.length(); i++) {
			char curChar = str.charAt(i);
			if (numPreEscapes == 0 && curChar == separator) { // separator
				return i;
			} else {
				split.append(curChar);
				numPreEscapes = (curChar == escapeChar) ? (++numPreEscapes) % 2 : 0;
			}
		}
		return -1;
	}

	/**
	 * Escape commas in the string using the default escape char
	 * 
	 * @param str
	 *            a string
	 * @return an escaped string
	 */
	public static String escapeString(String str) {
		return escapeString(str, ESCAPE_CHAR, COMMA);
	}

	/**
	 * Escape <code>charToEscape</code> in the string with the escape char
	 * <code>escapeChar</code>
	 * 
	 * @param str
	 *            string
	 * @param escapeChar
	 *            escape char
	 * @param charToEscape
	 *            the char to be escaped
	 * @return an escaped string
	 */
	public static String escapeString(String str, char escapeChar, char charToEscape) {
		return escapeString(str, escapeChar, new char[] { charToEscape });
	}

	// check if the character array has the character
	private static boolean hasChar(char[] chars, char character) {
		for (char target : chars) {
			if (character == target) {
				return true;
			}
		}
		return false;
	}

	/**
	 * @param charsToEscape
	 *            array of characters to be escaped
	 */
	public static String escapeString(String str, char escapeChar, char[] charsToEscape) {
		if (str == null) {
			return null;
		}
		int len = str.length();
		// Let us specify good enough capacity to constructor of StringBuilder
		// sothat
		// resizing would not be needed(to improve perf).
		StringBuilder result = new StringBuilder((int) (len * 1.5));

		for (int i = 0; i < len; i++) {
			char curChar = str.charAt(i);
			if (curChar == escapeChar || hasChar(charsToEscape, curChar)) {
				// special char
				result.append(escapeChar);
			}
			result.append(curChar);
		}
		return result.toString();
	}

	/**
	 * Unescape commas in the string using the default escape char
	 * 
	 * @param str
	 *            a string
	 * @return an unescaped string
	 */
	public static String unEscapeString(String str) {
		return unEscapeString(str, ESCAPE_CHAR, COMMA);
	}

	/**
	 * Unescape <code>charToEscape</code> in the string with the escape char
	 * <code>escapeChar</code>
	 * 
	 * @param str
	 *            string
	 * @param escapeChar
	 *            escape char
	 * @param charToEscape
	 *            the escaped char
	 * @return an unescaped string
	 */
	public static String unEscapeString(String str, char escapeChar, char charToEscape) {
		return unEscapeString(str, escapeChar, new char[] { charToEscape });
	}

	/**
	 * @param charsToEscape
	 *            array of characters to unescape
	 */
	public static String unEscapeString(String str, char escapeChar, char[] charsToEscape) {
		if (str == null) {
			return null;
		}
		StringBuilder result = new StringBuilder(str.length());
		boolean hasPreEscape = false;
		for (int i = 0; i < str.length(); i++) {
			char curChar = str.charAt(i);
			if (hasPreEscape) {
				if (curChar != escapeChar && !hasChar(charsToEscape, curChar)) {
					// no special char
					throw new IllegalArgumentException("Illegal escaped string " + str + " unescaped " + escapeChar +
							" at " + (i - 1));
				}
				// otherwise discard the escape char
				result.append(curChar);
				hasPreEscape = false;
			} else {
				if (hasChar(charsToEscape, curChar)) {
					throw new IllegalArgumentException("Illegal escaped string " + str + " unescaped " + curChar +
							" at " + i);
				} else if (curChar == escapeChar) {
					hasPreEscape = true;
				} else {
					result.append(curChar);
				}
			}
		}
		if (hasPreEscape) {
			throw new IllegalArgumentException("Illegal escaped string " + str + ", not expecting " + escapeChar +
					" in the end.");
		}
		return result.toString();
	}

	/**
	 * Return hostname without throwing exception.
	 * 
	 * @return hostname
	 */
	public static String getHostname() {
		try {
			return "" + InetAddress.getLocalHost();
		} catch (UnknownHostException uhe) {
			return "" + uhe;
		}
	}

	/**
	 * Return a message for logging.
	 * 
	 * @param prefix
	 *            prefix keyword for the message
	 * @param msg
	 *            content of the message
	 * @return a message for logging
	 */
	private static String toStartupShutdownString(String prefix, String[] msg) {
		StringBuffer b = new StringBuffer(prefix);
		b.append("\n/************************************************************");
		for (String s : msg)
			b.append("\n" + prefix + s);
		b.append("\n************************************************************/");
		return b.toString();
	}

	/**
	 * Print a log message for starting up and shutting down
	 * 
	 * @param clazz
	 *            the class of the server
	 * @param args
	 *            arguments
	 * @param LOG
	 *            the target log object
	 */
	public static void startupShutdownMessage(Class<?> clazz, String[] args, final org.apache.commons.logging.Log LOG) {
		final String hostname = getHostname();
		final String classname = clazz.getSimpleName();
		LOG.info(toStartupShutdownString(
				"STARTUP_MSG: ",
				new String[] {
						"Starting " + classname,
						"  host = " + hostname,
						"  args = " + Arrays.asList(args),
						"  version = " + VersionInfo.getVersion(),
						"  build = " + VersionInfo.getUrl() + " -r " + VersionInfo.getRevision() + "; compiled by '" +
								VersionInfo.getUser() + "' on " + VersionInfo.getDate(),
						"  java = " + System.getProperty("java.version") }));

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				LOG.info(toStartupShutdownString("SHUTDOWN_MSG: ", new String[] { "Shutting down " + classname +
						" at " + hostname }));
			}
		});
	}

	/**
	 * The traditional binary prefixes, kilo, mega, ..., exa, which can be
	 * represented by a 64-bit integer. TraditionalBinaryPrefix symbol are case
	 * insensitive.
	 */
	public static enum TraditionalBinaryPrefix {
		KILO(1024), MEGA(KILO.value << 10), GIGA(MEGA.value << 10), TERA(GIGA.value << 10), PETA(TERA.value << 10), EXA(
				PETA.value << 10);

		public final long value;
		public final char symbol;

		TraditionalBinaryPrefix(long value) {
			this.value = value;
			this.symbol = toString().charAt(0);
		}

		/**
		 * @return The TraditionalBinaryPrefix object corresponding to the
		 *         symbol.
		 */
		public static TraditionalBinaryPrefix valueOf(char symbol) {
			symbol = Character.toUpperCase(symbol);
			for (TraditionalBinaryPrefix prefix : TraditionalBinaryPrefix.values()) {
				if (symbol == prefix.symbol) {
					return prefix;
				}
			}
			throw new IllegalArgumentException("Unknown symbol '" + symbol + "'");
		}

		/**
		 * Convert a string to long. The input string is first be trimmed and
		 * then it is parsed with traditional binary prefix.
		 * 
		 * For example, "-1230k" will be converted to -1230 * 1024 = -1259520;
		 * "891g" will be converted to 891 * 1024^3 = 956703965184;
		 * 
		 * @param s
		 *            input string
		 * @return a long value represented by the input string.
		 */
		public static long string2long(String s) {
			s = s.trim();
			final int lastpos = s.length() - 1;
			final char lastchar = s.charAt(lastpos);
			if (Character.isDigit(lastchar))
				return Long.parseLong(s);
			else {
				long prefix = TraditionalBinaryPrefix.valueOf(lastchar).value;
				long num = Long.parseLong(s.substring(0, lastpos));
				if (num > (Long.MAX_VALUE / prefix) || num < (Long.MIN_VALUE / prefix)) {
					throw new IllegalArgumentException(s + " does not fit in a Long");
				}
				return num * prefix;
			}
		}
	}

	/**
	 * Escapes HTML Special characters present in the string.
	 * 
	 * @param string
	 * @return HTML Escaped String representation
	 */
	public static String escapeHTML(String string) {
		if (string == null) {
			return null;
		}
		StringBuffer sb = new StringBuffer();
		boolean lastCharacterWasSpace = false;
		char[] chars = string.toCharArray();
		for (char c : chars) {
			if (c == ' ') {
				if (lastCharacterWasSpace) {
					lastCharacterWasSpace = false;
					sb.append("&nbsp;");
				} else {
					lastCharacterWasSpace = true;
					sb.append(" ");
				}
			} else {
				lastCharacterWasSpace = false;
				switch (c) {
				case '<':
					sb.append("&lt;");
					break;
				case '>':
					sb.append("&gt;");
					break;
				case '&':
					sb.append("&amp;");
					break;
				case '"':
					sb.append("&quot;");
					break;
				default:
					sb.append(c);
					break;
				}
			}
		}

		return sb.toString();
	}

	/**
	 * Return an abbreviated English-language desc of the byte length
	 */
	public static String byteDesc(long len) {
		double val = 0.0;
		String ending = "";
		if (len < 1024 * 1024) {
			val = (1.0 * len) / 1024;
			ending = " KB";
		} else if (len < 1024 * 1024 * 1024) {
			val = (1.0 * len) / (1024 * 1024);
			ending = " MB";
		} else if (len < 1024L * 1024 * 1024 * 1024) {
			val = (1.0 * len) / (1024 * 1024 * 1024);
			ending = " GB";
		} else if (len < 1024L * 1024 * 1024 * 1024 * 1024) {
			val = (1.0 * len) / (1024L * 1024 * 1024 * 1024);
			ending = " TB";
		} else {
			val = (1.0 * len) / (1024L * 1024 * 1024 * 1024 * 1024);
			ending = " PB";
		}
		return limitDecimalTo2(val) + ending;
	}

	public static synchronized String limitDecimalTo2(double d) {
		return decimalFormat.format(d);
	}

	/**
	 * Concatenates strings, using a separator.
	 * 
	 * @param separator
	 *            Separator to join with.
	 * @param strings
	 *            Strings to join.
	 * @return the joined string
	 */
	public static String join(CharSequence separator, Iterable<String> strings) {
		if (strings == null)
			return "";
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String s : strings) {
			if (first) {
				first = false;
			} else {
				sb.append(separator);
			}
			sb.append(s);
		}
		return sb.toString();
	}

	/**
	 * Concatenates strings, using a separator.
	 * 
	 * @param separator
	 *            to join with
	 * @param strings
	 *            to join
	 * @return the joined string
	 */
	public static String join(CharSequence separator, String[] strings) {
		// Ideally we don't have to duplicate the code here if array is
		// iterable.
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String s : strings) {
			if (first) {
				first = false;
			} else {
				sb.append(separator);
			}
			sb.append(s);
		}
		return sb.toString();
	}

	/**
	 * Concatenates objects, using a separator.
	 * 
	 * @param separator
	 *            to join with
	 * @param objects
	 *            to join
	 * @return the joined string
	 */
	public static String join(CharSequence separator, Object[] objects) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Object obj : objects) {
			if (first) {
				first = false;
			} else {
				sb.append(separator);
			}
			sb.append(obj);
		}
		return sb.toString();
	}

	/**
	 * Capitalize a word
	 * 
	 * @param s
	 *            the input string
	 * @return capitalized string
	 */
	public static String capitalize(String s) {
		int len = s.length();
		if (len == 0)
			return s;
		return new StringBuilder(len).append(Character.toTitleCase(s.charAt(0))).append(s.substring(1)).toString();
	}

	/**
	 * Convert SOME_STUFF to SomeStuff
	 * 
	 * @param s
	 *            input string
	 * @return camelized string
	 */
	public static String camelize(String s) {
		StringBuilder sb = new StringBuilder();
		String[] words = split(s.toLowerCase(Locale.US), ESCAPE_CHAR, '_');

		for (String word : words)
			sb.append(capitalize(word));

		return sb.toString();
	}
}
