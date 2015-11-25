package com.bi.common.util;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bi.common.init.ConstantEnum;

public class MACFormatUtil {

	public static Map<ConstantEnum, String> macFormat(String macStr) {
		Map<ConstantEnum, String> macForamtInfoMap = new WeakHashMap<ConstantEnum, String>();
		try {
			String macFilterColonStr = macFormatToCorrectStr(macStr);// macStr.replaceAll(":",
																		// "").toUpperCase();
			// System.out.println(macFilterColonStr);
			String macLongStr = Long.valueOf(macFilterColonStr, 16).toString();
			macForamtInfoMap.put(ConstantEnum.MAC, macStr);
			macForamtInfoMap.put(ConstantEnum.MAC_LONG, macLongStr);
		} catch (Exception e) {
			// e.printStackTrace();
			macForamtInfoMap.put(ConstantEnum.MAC, macStr);
			macForamtInfoMap.put(ConstantEnum.MAC_LONG, 0l + "");
		}
		return macForamtInfoMap;
	}

	/**
	 * 
	 * @param macStr
	 * @return
	 * @throws Exception
	 */
	public static void isCorrectMac(String macStr) throws Exception {
		String macRex = "[0-9a-fA-F]{32}|"
				+ "([0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2})|"
				+ "[0-9a-fA-F]{12}|"
				+ "[0-9a-fA-F]{8}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{12}"
				+ "|null|\\(null\\)|NULL|\\(NULL\\)";
		Pattern pattern = Pattern.compile(macRex);
		Matcher matcher = pattern.matcher(macStr);
		if (!matcher.matches() && !macStr.equalsIgnoreCase("")) {
			throw new Exception("MAC 地址不簿符合格式规范" + macRex);
		}
	}

//	public static String getDefaultMac(String macStr) {
//		if ("".equalsIgnoreCase(macStr)) {
//			macStr = "000000000000";
//		}
//
//		return macStr;
//	}

	public static String macFormatToCorrectStr(String macStr) {
		String returnMacStr = macStr;
		if (macStr.contains(":")) {
			returnMacStr = macStr.replaceAll(":", "").toUpperCase();
		}
		if (macStr.contains(".")) {
			returnMacStr = macStr.replaceAll("\\.", "").toUpperCase();
		}
		return returnMacStr;

	}
	
	
}
