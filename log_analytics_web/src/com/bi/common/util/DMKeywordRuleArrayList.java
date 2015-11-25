package com.bi.common.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bi.common.dm.pojo.DMKeywordRule;

public class DMKeywordRuleArrayList<E>{
	/**
	 * 
	 */
	private ArrayList<E> arrayList = null;

	public DMKeywordRuleArrayList(ArrayList<E> arrayList) {
		this.arrayList = arrayList;

	}

	public void add(E e){
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
					ch = (char) Integer.parseInt(src.substring(pos + 2, pos + 6), 16); 
					tmp.append(ch); 
					lastPos = pos + 6; 
				} 
				else { 
					ch = (char) Integer.parseInt(src.substring(pos + 1, pos + 3), 16); 
					tmp.append(ch); 
					lastPos = pos + 3; 
				} 
			} 
			else { 
				if (pos == -1) { 
					tmp.append(src.substring(lastPos)); 
					lastPos = src.length(); 
				}
				else { 
					tmp.append(src.substring(lastPos, pos)); 
					lastPos = pos; 
				} 
			} 
		} 
		return tmp.toString(); 
	} 
	
	
	public String  getDmKeywordRule(String url){
		//String keywordPreReg = "(?:.+?[\\?|&]p=||";
		String keywordPreReg = "(?:.+?";
		String keywordSufReg = ")([^&]*)";
			
		for (int i = 0; i < this.arrayList.size(); i++) {
			DMKeywordRule dmKeywordRule = (DMKeywordRule) this.arrayList.get(i);
			String keywordReg = keywordPreReg.concat(dmKeywordRule.getKeyTemplate()).concat(keywordSufReg);		
			String encodeReg = "^(?:[\\x00-\\x7f]|[\\xfc-\\xff][\\x80-\\xbf]{5}|[\\xf8-\\xfb][\\x80-\\xbf]{4}|[\\xf0-\\xf7][\\x80-\\xbf]{3}|[\\xe0-\\xef][\\x80-\\xbf]{2}|[\\xc0-\\xdf][\\x80-\\xbf])+$"; 
		
			Pattern keywordPatt = Pattern.compile(keywordReg); 
			StringBuffer keyword = new StringBuffer(30); 
			Matcher keywordMat = keywordPatt.matcher(url); 
		
			while (keywordMat.find()) { 
				keywordMat.appendReplacement(keyword, "$1"); 
			} 
			
			if (!keyword.toString().equals("") && url.contains(dmKeywordRule.getUrlPrefix())){ 
				String keywordsTmp = keyword.toString().replace(dmKeywordRule.getUrlPrefix(),""); 
			    
				/*
				System.out.println("url = "+ url );
				System.out.println("urlprefix ="+ dmKeywordRule.getUrlPrefix() );
				System.out.println("keywordTemplate ="+ dmKeywordRule.getKeyTemplate());
				System.out.println("keywordsTmp ="+ keywordsTmp );
				*/
				
				Pattern encodePatt = Pattern.compile(encodeReg); 
				String unescapeString = unescape(keywordsTmp); 
				Matcher encodeMat = encodePatt.matcher(unescapeString);
			
				String encodeString = "gbk"; 
				if (encodeMat.matches())
					encodeString = "utf-8"; 
				
				try { 
					return URLDecoder.decode(keywordsTmp, encodeString); 
				} catch (UnsupportedEncodingException e) { 
					return ""; 
				} 
			}			
		} 
		return ""; 
	}	
	
}


