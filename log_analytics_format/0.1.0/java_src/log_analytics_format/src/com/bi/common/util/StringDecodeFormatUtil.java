package com.bi.common.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringDecodeFormatUtil {
	public static String changeCharset(String str, String sourceCharset,
			String targetCharset) throws UnsupportedEncodingException {
		String targetStr = null;
		if (str != null) {
			byte[] byteString = str.getBytes(sourceCharset);
			targetStr = new String(byteString, targetCharset);
		}
		return targetStr;
	}

	public static String changeCharset(String str, String targetCharset)
			throws UnsupportedEncodingException {
		String targetStr = null;
		if (str != null) {
			byte[] byteString = str.getBytes();
			targetStr = new String(byteString, targetCharset);
		}
		return targetStr;
	}

	public static String decodeCodedStr(String sourceStr, String sourceCharset,
			String targetCharset) throws UnsupportedEncodingException {
		String decodedStr;
		String changedStr = changeCharset(sourceStr, sourceCharset,
				targetCharset);
		if (changedStr != null) {
			try {
				decodedStr = URLDecoder.decode(
						URLDecoder.decode(changedStr, targetCharset),
						targetCharset);
			} catch (Exception e) {
				decodedStr = "";
			}
			return decodedStr;
		}
		return "";
	}

	public static String decodeCodedStr(String sourceStr, String targetCharset)
			throws UnsupportedEncodingException{
		String decodedStr;
		String changedStr = sourceStr; // String changedStr =
										// changeCharset(sourceStr,
										// targetCharset);
		if (changedStr != null) {
			try {
				decodedStr = URLDecoder.decode(
						URLDecoder.decode(changedStr, targetCharset),
						targetCharset);
				//decodedStr = URLDecoder.decode(changedStr, targetCharset);
			} catch (Exception e) {
				decodedStr = "";
				return decodedStr;
			}
			return decodedStr;
		}
		return "";
	}

	public static String decodeFromHex(String sourceStr) {
		Pattern p = Pattern.compile("%u([a-zA-Z0-9]{4})");
		Matcher m = p.matcher(sourceStr);
		StringBuffer sb = new StringBuffer();
		while (m.find()) {
			m.appendReplacement(sb,
					String.valueOf((char) Integer.parseInt(m.group(1), 16)));
		}
		m.appendTail(sb);
		return sb.toString();
	}

}

