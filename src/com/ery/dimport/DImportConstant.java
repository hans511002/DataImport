package com.ery.dimport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;

import com.googlecode.aviator.AviatorEvaluator;

public abstract class DImportConstant {
	public static final String RESOURCES_KEY = "rescource.configuration.files";
	public static final String CONFIG_DB_DRIVER_KEY = "config.db.driver";
	public static final String CONFIG_DB_URL_KEY = "config.db.connecturl";
	public static final String CONFIG_DB_USER_KEY = "config.db.connectuser";
	public static final String CONFIG_DB_PASS_KEY = "config.db.connectpass";
	public static final String DIMPORT_INFO_PORT_KEY = "dimport.info.port";
	public static final String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout";
	public static final int DEFAULT_ZK_SESSION_TIMEOUT = 60000;
	public static final String ZOOKEEPER_BASE_ZNODE = "dimport.zookeeper.baseznode";
	public static final String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/dimport";
	public static final String ZOOKEEPER_QUORUM = "dimport.zookeeper.quorum";
	public static final String ZOOKEEPER_USEMULTI = "zookeeper.useMulti";
	public static final int SOCKET_RETRY_WAIT_MS = 200;
	public static final String DIMPORT_PROCESS_THREAD_NUMBER = "dimport.process.thread.number";// 并发处理线程数
	public static final int DEFAULT_DIMPORT_PROCESS_THREAD_NUMBER = 10;
	public static final String DIMPORT_PROCESS_TMPDATA_DIR = "dimport.process.tmp.data.dir";
	public static final String DIMPORT_LOG_WRITE_LEVEL = "dimport.log.write.level";
	public static final String DIMPORT_LOG_WRITE_FLUASH_LENGTH = "dimport.log.write.flush.length";
	public static final String DIMPORT_LOG_WRITE_FLUASH_INTERVAL = "dimport.log.write.flush.interval";

	public static ObjectMapper objectMapper = new ObjectMapper();
	public static java.text.SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static java.text.SimpleDateFormat shdf = new SimpleDateFormat("yyyyMMddHHmmss");
	public static long utsTiime = new Date(70, 1, 1).getTime();
	public static String utsTiimeString = DImportConstant.sdf.format(new Date(70, 0, 1));

	/**
	 * <pre>
	 * 1:调试网络连接，不检查Storm进程,模拟Node网络连接
	 * 2：本地模式运行TOP
	 * 4:内置启动top
	 * </pre>
	 */
	public static int DebugType = 0;

	public static String getUserName() {
		return System.getProperty("user.name");
	}

	public static String getUserDir() {
		return System.getProperty("user.dir");
	}

	public static String getClassPath() {
		return System.getProperty("java.class.path");
	}

	public static String getHostName() {
		return System.getProperty("java.class.path");
	}

	public static <T> T castObject(byte[] data) throws IOException {
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
		ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		try {
			T a = (T) objectInputStream.readObject();
			return a;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			objectInputStream.close();
			byteArrayInputStream.close();
		}
	}

	public static byte[] Serialize(Object obj) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
		objectOutputStream.writeObject(obj);
		byte[] bts = byteArrayOutputStream.toByteArray();
		objectOutputStream.close();
		byteArrayOutputStream.close();
		return bts;
	}

	public static <T> T Clone(Object obj) throws IOException {
		return castObject(Serialize(obj));
	}

	/**
	 * 常规时间宏变量，使用动态表达式执行
	 * 
	 */
	public static final Pattern evalPattern = Pattern.compile("eval\\(.*\\)");

	public static String macroProcess(String str) {
		Matcher m = evalPattern.matcher(str);
		while (m.find()) {
			String exp = m.group(1);
			Object obj = AviatorEvaluator.execute(exp);
			str = m.replaceFirst(obj.toString());
			m = evalPattern.matcher(str);
		}
		return str;
	}

	public static Object convertToExecEnvObj(String value) {
		try {
			if (value.indexOf('.') >= 0) {
				return Double.parseDouble(value);
			} else {
				return Long.parseLong(value);
			}
		} catch (NumberFormatException e) {
			return value;
		}
	}

	public static boolean getEexcBoolean(Object obj, String[] row, Map<String, String> macroVariableMap)
			throws IOException {
		boolean res = false;
		try {
			if (obj instanceof Boolean) {
				res = (Boolean) obj;
			} else if (obj instanceof Integer) {
				res = ((Integer) obj > 0);
			} else if (obj instanceof Long) {
				res = ((Long) obj > 0);
			} else if (obj instanceof Double) {
				res = ((Double) obj > 0.000001);
			} else if (obj instanceof Float) {
				res = ((Float) obj > 0.000001);
			} else if (obj instanceof String) {

			}
		} catch (Exception e) {
			throw new IOException("计算失败，表达式或变量异常:" + e.getMessage());
		}
		return res;
	}

	public static Object castAviatorObject(String value) {
		try {
			if (value.indexOf('.') >= 0) {
				return Double.parseDouble(value);
			} else {
				return Long.parseLong(value);
			}
		} catch (NumberFormatException e) {
			return value;
		}
	}

	public static Object castAviatorObject(byte[] bt) {
		return castAviatorObject(new String(bt));
	}

	public static boolean getAviatorBoolean(Object obj) throws IOException {
		if (obj instanceof Boolean) {
			return (Boolean) obj;
		} else if (obj instanceof Integer) {
			return ((Integer) obj > 0);
		} else if (obj instanceof Long) {
			return ((Long) obj > 0);
		} else if (obj instanceof Double) {
			return ((Double) obj > 0.000001);
		} else if (obj instanceof Float) {
			return ((Float) obj > 0.000001);
		} else if (obj instanceof String) {
			if (obj != null && !obj.toString().trim().equals(""))
				return true;
			else
				return false;
		} else {
			return obj != null;
		}
	}

	public static enum SeqType {
		NodeLogId,
	}
}
