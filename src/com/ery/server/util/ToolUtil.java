/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.ery.server.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;

public class ToolUtil {
	public static final String STAT_JOBS = "jobs";
	public static final String STAT_COUNTERS = "counters";

	public static final Map<String, Object> toArgMap(Object... args) {
		if (args == null) {
			return null;
		}
		if (args.length % 2 != 0) {
			throw new RuntimeException("expected pairs of argName argValue");
		}
		HashMap<String, Object> res = new HashMap<String, Object>();
		for (int i = 0; i < args.length; i += 2) {
			if (args[i + 1] != null) {
				res.put(String.valueOf(args[i]), args[i + 1]);
			}
		}
		return res;
	}

	/**
	 * 将对象序列化成字符串
	 * 
	 * @param obj
	 * @return
	 * @throws IOException
	 */
	public static String serialObject(Object obj) throws IOException {
		return serialObject(obj, false, false);
	}

	public static String serialObject(Object obj, boolean isGzip, boolean urlEnCode) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
		objectOutputStream.writeObject(obj);
		String serStr = null;
		byte[] bts = null;
		if (isGzip) {
			bts = GZIPUtils.zip(byteArrayOutputStream.toByteArray());
		} else {
			bts = byteArrayOutputStream.toByteArray();
		}
		if (urlEnCode) {
			serStr = new String(org.apache.commons.codec.binary.Base64.encodeBase64(bts), "ISO-8859-1");
		} else {
			serStr = new String(bts, "ISO-8859-1");
		}
		objectOutputStream.close();
		byteArrayOutputStream.close();
		return serStr;
	}

	/**
	 * 反序列化对象
	 * 
	 * @param serStr
	 * @return
	 * @throws IOException
	 */
	public static Object deserializeObject(String serStr) throws IOException {
		return deserializeObject(serStr, false, false);
	}

	public static Object deserializeObject(String serStr, boolean isGzip, boolean urlEnCode) throws IOException {
		byte[] bts = null;
		if (urlEnCode) {
			bts = org.apache.commons.codec.binary.Base64.decodeBase64(serStr.getBytes("ISO-8859-1"));
		} else {
			bts = serStr.getBytes("ISO-8859-1");
		}
		if (isGzip)
			bts = GZIPUtils.unzip(bts);
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bts);
		ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		try {
			return objectInputStream.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			objectInputStream.close();
			byteArrayInputStream.close();
		}
	}

	public static synchronized Process runProcess(String command, Log log) throws IOException {
		log.info("调用外部命令：" + command);
		// Process proc = Runtime.getRuntime().exec(new String[] { command });
		Process proc = Runtime.getRuntime().exec(command);
		return proc;
	}

	public static synchronized Process runProcess(String command) throws IOException {
		Process proc = Runtime.getRuntime().exec(command);
		return proc;
	}

	public static synchronized void killProcess(Process proc) {
		proc.destroy();
	}

	public static final Pattern valPartsRegex = Pattern.compile("\\$(\\d+)");

	// 正则表达式的定向替换
	public static String ReplaceRegex(Matcher m, String substitution) {
		try {
			Matcher vm = valPartsRegex.matcher(substitution);
			String val = substitution;
			String regpar = substitution;
			int gl = m.groupCount();
			while (vm.find()) {
				regpar = regpar.substring(vm.end());
				int g = Integer.parseInt(vm.group(1));
				if (g > gl) {
					val = val.replaceAll("\\$\\d", "");
					break;
				}
				String gv = m.group(Integer.parseInt(vm.group(1)));
				if (gv != null)
					val = val.replaceAll("\\$" + g, gv);
				else
					val = val.replaceAll("\\$" + g, "");
				vm = valPartsRegex.matcher(regpar);
			}
			return val;
		} catch (Exception e) {
			return null;
		}
	}

	public static String getPath(Path path) {
		try {
			URI aUri = new URI(path.toString());
			return aUri.getPath();
		} catch (URISyntaxException e) {
			return path.toString();
		}
	}

	public static void close(Closeable c) {
		if (c != null) {
			try {
				c.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void close(Socket c) {
		if (c != null) {
			try {
				c.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static long toLong(String val) {
		long l = 0;
		for (char c : val.toCharArray()) {
			if (Character.isDigit(c))
				l = l * 10 + c - 48;
			else
				break;
		}
		return l;
	}

	public static Double toDouble(String val) {
		Double l = 0.0;
		boolean dig = false;
		long len = 10;
		long digs = 0;
		for (char c : val.toCharArray()) {
			if (Character.isDigit(c)) {
				if (dig) {
					digs = digs * 10 + c - 48;
					len *= 10;
				} else {
					l = l * 10 + c - 48;
				}
			} else if (c == '.') {
				dig = true;
			} else {
				break;
			}
		}
		return l + (double) digs * 10 / len;
	}

	public static boolean IsInt(String s) {
		boolean result = false;
		try {
			Integer.parseInt(s);
			result = true;
		} catch (Exception e) {
		}

		return result;
	}

	public static int str2Int(String s) {
		int result = 0;
		try {
			result = Integer.parseInt(s);
		} catch (Exception e) {
			result = -1;
		}
		return result;
	}

	// ��������
	public static String parseChinese(String in) {

		String s = null;
		byte temp[];
		if (in == null) {
			System.out.println("Warn:Chinese null founded!");
			return new String("");
		}
		try {
			temp = in.getBytes("iso-8859-1");
			s = new String(temp, "GBK");
		} catch (UnsupportedEncodingException e) {
			System.out.println("���ֱ���ת�����?" + e.toString());
		}
		return s;
	}

	// ��������
	public static String parseISO(String in) {

		String s = null;
		byte temp[];
		if (in == null) {
			System.out.println("Warn:Chinese null founded!");
			return new String("");
		}
		try {
			temp = in.getBytes("GBK");
			s = new String(temp, "iso-8859-1");

		} catch (UnsupportedEncodingException e) {
			System.out.println("���ֱ���ת�����?" + e.toString());

		}
		return s;

	}
}
