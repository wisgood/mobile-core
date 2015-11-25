package com.bi.extend.util;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bi.common.constrants.UtilComstrantsEnum;

public class MACFormatUtil {

	private Pattern pattern;

	public MACFormatUtil() {
		this.pattern = Pattern.compile(UtilComstrantsEnum.macRex.getValueStr());

	}

	public Map<String, String> macFormat(String macStr) {
		Map<String, String> macForamtInfoMap = new WeakHashMap<String, String>();
		try {
			String macFilterColonStr = macFormatToCorrectStr(macStr);
			String macLongStr = Long.valueOf(macFilterColonStr, 16).toString();
			macForamtInfoMap.put(UtilComstrantsEnum.mac.getValueStr(), macStr);
			macForamtInfoMap.put(UtilComstrantsEnum.macCode.getValueStr(),
					macLongStr);
		} catch (Exception e) {
			macForamtInfoMap.put(UtilComstrantsEnum.mac.getValueStr(), macStr);
			macForamtInfoMap.put(UtilComstrantsEnum.macCode.getValueStr(),
					0l + "");
		}
		return macForamtInfoMap;
	}

	/**
	 * 
	 * @param macStr
	 * @return
	 * @throws Exception
	 */
	public void isCorrectMac(String macStr) throws Exception {
		Matcher matcher = pattern.matcher(macStr);
		if (!matcher.matches() && !macStr.equalsIgnoreCase("")) {
			throw new Exception("MAC address errors");
		}
	}

	public String macFormatToCorrectStr(String macStr) {
		String returnMacStr = macStr;
		if (macStr.contains(":")) {
			returnMacStr = macStr.replaceAll(":", "");
		}
		if (macStr.contains(".")) {
			returnMacStr = macStr.replaceAll("\\.", "");
		}
		return returnMacStr.toUpperCase();

	}
}
