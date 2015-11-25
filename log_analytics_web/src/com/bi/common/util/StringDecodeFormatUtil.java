package com.bi.common.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class StringDecodeFormatUtil {
	public static String changeCharset(String str, String sourceCharset, String targetCharset) 
			throws UnsupportedEncodingException{
		String targetStr = null;
		if(str != null) {
			byte[] byteString = str.getBytes(sourceCharset);
			targetStr = new String(byteString, targetCharset);       
		}
	   return targetStr;
	}
	public static String changeCharset(String str, String targetCharset) 
			throws UnsupportedEncodingException{
		String targetStr = null;
		if(str != null) {
			byte[] byteString = str.getBytes();
			targetStr = new String(byteString, targetCharset);       
		}
	   return targetStr;
	}
	
	public static String decodeCodedStr(String sourceStr, String sourceCharset, String targetCharset)
			throws UnsupportedEncodingException{
		String decodedStr;
		String changedStr = changeCharset(sourceStr, sourceCharset, targetCharset);
		if(changedStr != null){
			try{
				decodedStr = URLDecoder.decode(URLDecoder.decode(changedStr,
						targetCharset), targetCharset);
			}catch(Exception e){	
					decodedStr="unknown";
			 }
			return decodedStr;
		}
		return null;
	}		
	public static String decodeCodedStr(String sourceStr, String targetCharset)
			throws UnsupportedEncodingException{
		String decodedStr;
		String changedStr = changeCharset(sourceStr, targetCharset);
		if(changedStr != null){
			try{
				decodedStr = URLDecoder.decode(URLDecoder.decode(changedStr,
						targetCharset), targetCharset);
			}catch(Exception e){	
					decodedStr="unknown";
			 }
			return decodedStr;
		}
		return null;
	}	

}
