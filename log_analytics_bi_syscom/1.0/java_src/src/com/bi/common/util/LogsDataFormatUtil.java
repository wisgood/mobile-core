package com.bi.common.util;

import java.util.List;

public class LogsDataFormatUtil {
	public static String mergeArrayBySign(List<String> fieldList, String sign) {
		StringBuilder mergeResultSb = new StringBuilder();
		int outputResultLength = fieldList.size();
		for (int i = 0; i < outputResultLength; i++) {
			mergeResultSb.append(fieldList.get(i));
			if (i < outputResultLength - 1) {
				mergeResultSb.append(sign);
			}
		}
		return mergeResultSb.toString();
	}
}
