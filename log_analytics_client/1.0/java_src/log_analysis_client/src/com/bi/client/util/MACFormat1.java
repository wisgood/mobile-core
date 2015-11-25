package com.bi.client.util;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MACFormat1 {

	private static  Pattern pattern = null;
	
	static {		
		 String macRex = "[0-9a-fA-F]{32}|"
					+ "([0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2})|"
					+ "[0-9a-fA-F]{12}|"
					+ "[0-9a-fA-F]{8}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{12}"
					+ "|null|\\(null\\)|NULL|\\(NULL\\)";
		 pattern = Pattern.compile(macRex);
	}
	
	public static String macFormat(String macStr) {
		String mac = "-";
		if(isCorrectMac(macStr))
			mac = macStr.toUpperCase();
		return mac;
	}


	public static boolean isCorrectMac(String macStr) {
		Matcher matcher = pattern.matcher(macStr);
		if (!matcher.matches() || macStr.equals(" ") || macStr.equals("")) 
			return false;				
		return true;
	}

	public static String macFormatToCorrectStr(String macStr) {
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
