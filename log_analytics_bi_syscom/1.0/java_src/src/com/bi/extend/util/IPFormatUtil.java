package com.bi.extend.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bi.common.constrants.UtilComstrantsEnum;

public class IPFormatUtil {

	private Pattern pattern;

	public IPFormatUtil() {
		this.pattern = Pattern.compile(UtilComstrantsEnum.ipFormatRegex
				.getValueStr());
	}

	public String ipFormat(String ipStr) {
		Matcher matcher = pattern.matcher(ipStr);
		boolean isExceptionIp = ipStr == null
				|| "".equalsIgnoreCase(ipStr.trim()) || !(matcher.matches());
		if (isExceptionIp) {
			ipStr = UtilComstrantsEnum.ipDefault.getValueStr();
		}
		return ipStr;
	}

}
