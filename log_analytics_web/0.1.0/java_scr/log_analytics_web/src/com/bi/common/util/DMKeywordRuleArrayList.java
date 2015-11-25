package com.bi.common.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bi.common.dm.pojo.DMKeywordRule;

public class DMKeywordRuleArrayList<E> {
	/**
	 * 
	 */
	private ArrayList<E> arrayList = null;

	public DMKeywordRuleArrayList(ArrayList<E> arrayList) {
		this.arrayList = arrayList;

	}

	public void add(E e) {
		this.arrayList.add(e);
	}

	public static String unescape(String src) {

		StringBuffer tmp = new StringBuffer();
		tmp.ensureCapacity(src.length());

		int lastPos = 0, pos = 0;
		char ch;
		while (lastPos < src.length()) {
			pos = src.indexOf("%", lastPos);
			if (pos == lastPos) {
				if (src.charAt(pos + 1) == 'u') {
					ch = (char) Integer.parseInt(
							src.substring(pos + 2, pos + 6), 16);
					tmp.append(ch);
					lastPos = pos + 6;
				} else {
					ch = (char) Integer.parseInt(
							src.substring(pos + 1, pos + 3), 16);
					tmp.append(ch);
					lastPos = pos + 3;
				}
			} else {
				if (pos == -1) {
					tmp.append(src.substring(lastPos));
					lastPos = src.length();
				} else {
					tmp.append(src.substring(lastPos, pos));
					lastPos = pos;
				}
			}
		}
		return tmp.toString();
	}

	public static String keyNormalizer(String key) {
		String keyStr = key;
		if (null != keyStr && !"".equals(keyStr)) {
			keyStr = keyStr.replace("\t", "");
			keyStr = keyStr.replace("\r\n", "");
			keyStr = keyStr.replace("\n", "");
			return keyStr;
		}
		return "";
	}

	public String getSearchKeyDecode(String searchKey) {
		String decodeReg = "^(?:[\\x00-\\x7f]|[\\xfc-\\xff][\\x80-\\xbf]{5}|[\\xf8-\\xfb][\\x80-\\xbf]{4}|[\\xf0-\\xf7][\\x80-\\xbf]{3}|[\\xe0-\\xef][\\x80-\\xbf]{2}|[\\xc0-\\xdf][\\x80-\\xbf])+$";
		Pattern decodePatt = Pattern.compile(decodeReg);
		String unescapeString = unescape(searchKey);
		Matcher decodeMat = decodePatt.matcher(unescapeString);

		String decodeType = "gbk";
		if (decodeMat.matches())
			decodeType = "utf-8";
		try {
			return URLDecoder.decode(searchKey, decodeType);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	public String getDmKeywordRule(String url) {
		String urlInfoStr[] = url.split("&");
		String keyword = "";
		String urlprefix = "";
		String keyTemplate = "";
		String templateArray[] = null;
		int beginIndex = -1;
		for (int i = 0; i < this.arrayList.size(); i++) {
			DMKeywordRule dmKeywordRule = (DMKeywordRule) this.arrayList.get(i);
			urlprefix = dmKeywordRule.getUrlPrefix();

			if (urlInfoStr[0].indexOf(urlprefix) != -1) {

				keyTemplate = dmKeywordRule.getKeyTemplate();
				templateArray = keyTemplate.split("/");
				/*
				 * System.out.println("key is "+ key);
				 * System.out.println("template is "+template );
				 * System.out.println("urlInfoStr[0] is "+urlInfoStr[0]);
				 */
				for (int j = 1; j < urlInfoStr.length; j++) {
					for (int k = 0; k < templateArray.length; k++) {
						beginIndex = urlInfoStr[j].indexOf(templateArray[k]);
						if (beginIndex != -1) {
							keyword = urlInfoStr[j].substring(beginIndex
									+ templateArray[k].length());
							return keyNormalizer(getSearchKeyDecode(keyword));
						}
					}
				}
			}
		}
		return "";
	}

}
