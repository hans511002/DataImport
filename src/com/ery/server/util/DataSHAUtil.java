package com.ery.server.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DataSHAUtil {
	public static final String KEY_SHA = "SHA";
	public static final String KEY_MD5 = "MD5";

	/**
	 * MD5加密
	 * 
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static byte[] encryptMD5(byte[] data) throws Exception {
		MessageDigest md5 = MessageDigest.getInstance(KEY_MD5);
		md5.update(data);

		return md5.digest();
	}

	/**
	 * SHA加密
	 * 
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static byte[] encryptSHA(byte[] data) throws Exception {
		MessageDigest sha = MessageDigest.getInstance(KEY_SHA);
		sha.update(data);

		return sha.digest();
	}

	/**
	 * SHA加密算法32
	 * 
	 * @param inputStr
	 * @return
	 */
	public static String shaEncrypt(String inputStr) {
		byte[] inputData = inputStr.getBytes();
		String returnString = "";
		try {
			// 将二进制转换成十六进制字符串
			returnString = byte2hex(encryptSHA(inputData));
		} catch (Exception e) {
			// ErrorLogger.warn("SHA加密算法32 shaEncrypt 出现异常!", e);
		}

		return returnString;
	}

	@SuppressWarnings("unused")
	private static String encrypte256(String plainText, String algorithm) {
		algorithm = "SHA-256";
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance(algorithm);
		} catch (NoSuchAlgorithmException e) {
			// ErrorLogger.warn("加密算法encrypte256 出现异常!", e);
		}
		md.update(plainText.getBytes());
		byte[] b = md.digest();
		StringBuilder output = new StringBuilder(32);
		for (int i = 0; i < b.length; i++) {
			String temp = Integer.toHexString(b[i] & 0xff);
			if (temp.length() < 2) {
				output.append("0");
			}
			output.append(temp);
		}
		return output.toString();
	}

	/**
	 * MD5的加密算法
	 * 
	 * @param inputStr
	 * @return
	 */
	public static String md5Encrypt(String inputStr) {
		byte[] inputData = inputStr.getBytes();
		String returnString = "";
		try {
			BigInteger md5 = new BigInteger(encryptMD5(inputData));
			returnString = md5.toString(16);

		} catch (Exception e) {
			// ErrorLogger.warn("加密算法md5Encrypt 出现异常!", e);
		}

		return returnString;
	}

	/**
	 * 二行制转字符串
	 * 
	 * @param b
	 * @return
	 */
	public static String byte2hex(byte[] b) {
		StringBuilder builder = new StringBuilder();
		for (int n = 0; n < b.length; n++) {
			String stmp = (java.lang.Integer.toHexString(b[n] & 0XFF));
			if (stmp.length() == 1) {
				builder.append("0").append(stmp);
			} else {
				builder.append(stmp);
			}
		}
		return builder.toString();
	}

	public static void main(String[] args) {
		// String inputStr =
		// "appkeycd527a244fc5a44bformatjsonmethoduser.loginuserName13535353536userPass123456v1.04arar5l4jsl9100q";

		// MRLog.systemOut(shaEncrypt(inputStr).length());
		// MRLog.systemOut(shaEncrypt(inputStr));

		// MRLog.systemOut(encrypte256("heisetoufa", "SHA-1").length());//
		// SHA-1算法
		// MRLog.systemOut(encrypte256("heisetoufa", "SHA-256").length());//
		// SHA-256算法
		// MRLog.systemOut(encrypte("heisetoufa", "SHA-512"));// SHA-512算法

		// MRLog.systemOut(encrypte("heisetoufa", "MD2"));// MD2算法
		// System.out.println(UUID.randomUUID());
		// MRLog.systemOut(md5Encrypt("/wanghao/mrtest/file/txt/nocodec/small/mrdata.txt"));// MD5算法
		long time = System.currentTimeMillis();
		System.out.println(time);
		for (int i = 0; i < 1; i++) {
			// UUID.randomUUID();
			md5Encrypt("7e479161-dc08-44be-b3ed-60a3ba9bec0d");
			// System.out.println();
		}
		System.out.println(System.currentTimeMillis() - time);
	}
}
