package com.bi.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebCheckUtil {
	public static boolean checkIsNum(String checkedStr){
		if(checkedStr.isEmpty()||null == checkedStr)
			return false;
		Pattern pattern = Pattern.compile("[0-9]*"); // .*[0-9]+//.[0-9]+
		Matcher matcher = pattern.matcher(checkedStr.trim());
		if (matcher.matches()){
			return true;
		}else {
			return false;
		}
	}
   
	public static String checkClientFlag(String clientflagStr){
		String clientflag = "7";
		if(checkIsNum(clientflagStr) == false)
			return clientflag;
		if(clientflagStr.equals("3")){
			clientflag = "2";
		} else if(clientflagStr.contains("21")){
			clientflag = "1";	
		} else{
			clientflag = clientflagStr;
		}
		return clientflag;
	}
	public static String getFieldNum(String numFieldStr, String defautValue){
		
		String val = numFieldStr;
		if (WebCheckUtil.checkIsNum(numFieldStr) == false) {
			val = defautValue;
		}
		return val;
	}
	
	public static String checkField(String fieldStr, String defautValue){
		String val = new String("");
		if(fieldStr.isEmpty()||null == fieldStr){
			val = defautValue;	
		} else{
			val = fieldStr.trim();
		}
		return val;
	}
	
	public static boolean checkFck(String fckSample){
		if(fckSample.isEmpty()||null == fckSample)
			return false;
		Pattern pat = Pattern.compile("[0-9]{9,9}[0-9a-z]{6,7}"); // .*[0-9]+//.[0-9]+
		Matcher matcher = pat.matcher(fckSample.trim().toLowerCase());
		if (matcher.matches()){
			return true;
		}else {
			return false;
		}
	}

}
