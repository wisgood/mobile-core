/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DIMInterURLDAOObj.java 
 * @Package com.bi.common.dm.pojo.dao 
 * @Description: Read InterURL file and Get URL three level Id
 * @author wanghh
 * @date 2013-6-18  
 */
package com.bi.common.dm.pojo.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bi.common.dm.constant.DMInterURLConst;
import com.bi.common.dm.constant.DMInterURLEnum;
import com.bi.common.init.ConstantEnum;
import com.bi.common.util.StringDecodeFormatUtil;

public class DMInterURLDAOObj {
	private static Logger logger = Logger.getLogger(DMInterURLDAOObj.class
			.getName());

	private Map<String, HashMap<String, HashMap<String, String>>> dmInUrlMap = null;

	@SuppressWarnings("unchecked")
	public void parseDMObj(File file) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader inFile = null;
		try {
			this.dmInUrlMap = new HashMap<String, HashMap<String, HashMap<String, String>>>();
			@SuppressWarnings({ "unchecked", "rawtypes" })
			HashMap[] levelMaps = new HashMap[5];
			for (int i = 0; i < DMInterURLConst.INTER_URL_FIRST_LEVEL_NUM; i++) {
				levelMaps[i] = new HashMap<String, HashMap<String, String>>();
			}
			inFile = new BufferedReader(new InputStreamReader(
					new FileInputStream(file)));
			String line;
			while ((line = inFile.readLine()) != null) {
				String[] strURLLine = line.trim().split("\t");
				String[] strURL = strURLLine[DMInterURLEnum.URL.ordinal()]
						.trim().split("/");
				String strSecondName = new String();
				String strThirdName = new String();
				String strThird = new String();
				if (strURL.length == 1) {
					strSecondName = DMInterURLConst.INTER_URL_NULL_LEVEL;
					strThirdName = DMInterURLConst.INTER_URL_NULL_LEVEL;
				} else if (strURL.length == 2) {
					strSecondName = strURL[1].trim();
					strThirdName = DMInterURLConst.INTER_URL_NULL_LEVEL;
				} else if (strURL.length == 3) {
					if (strURL[2].equals("0")) {
						strThirdName = "n";
					} else {
						strThirdName = strURL[2];
					}
					strSecondName = strURL[1].trim();
				}
				strThird = strURLLine[DMInterURLEnum.SECOND_ID.ordinal()] + "#"
						+ strURLLine[DMInterURLEnum.THIRD_ID.ordinal()];

				int index = Integer.parseInt(strURLLine[DMInterURLEnum.FIRST_ID
						.ordinal()]) - 1;
				if (index > DMInterURLConst.INTER_URL_FIRST_LEVEL_NUM - 1)
					break;
				if (levelMaps[index].containsKey(strSecondName)) {
					HashMap<String, String> thirdMap = (HashMap<String, String>) levelMaps[index]
							.get(strSecondName);
					thirdMap.put(strThird, strThirdName);
					levelMaps[index].put(strSecondName, thirdMap);
				} else {
					HashMap<String, String> thirdNewMap = new HashMap<String, String>();
					thirdNewMap.put(strThird, strThirdName);
					levelMaps[index].put(strSecondName, thirdNewMap);
				}
			}
			for (int j = 1; j < DMInterURLConst.INTER_URL_FIRST_LEVEL_NUM + 1; j++) {
				this.dmInUrlMap.put(String.valueOf(j), levelMaps[j - 1]);
			}
		} catch (Exception e) {
			logger.error("站内url维度文件读取错误:"+e.getMessage(), e.getCause());
		} finally {
			inFile.close();
		}
	}

	public Map<ConstantEnum, String> getDMOjb(String param) {
		// TODO Auto-generated method stub
		Map<ConstantEnum, String> URLTypeMap = new WeakHashMap<ConstantEnum, String>();
		String strInputURL = param;
		if (strInputURL == null || strInputURL.isEmpty()) {
			
			URLTypeMap.put(ConstantEnum.URL_FIRST_ID, "-1");
			URLTypeMap.put(ConstantEnum.URL_SECOND_ID, "-1");
			URLTypeMap.put(ConstantEnum.URL_THIRD_ID, "-1");
			
		} else {
			
			if (strInputURL.startsWith("http")) {
				String[] strInURLLevel = new String[DMInterURLConst.INTER_URL_LEVEL_NUM];
				String[] strURLLevel = strInputURL.trim().split("/");
				
				if (strURLLevel.length >= 5 && strURLLevel[3].equals("search")&& !strURLLevel[4].equals("list")) {
					Map<String, String> searchResMap = getSearchExt(strURLLevel[strURLLevel.length - 1]);
					if (searchResMap != null) {
						URLTypeMap.put(ConstantEnum.URL_KEYWORD,
								searchResMap.get("KEYWORD"));
						URLTypeMap.put(ConstantEnum.URL_PAGE, searchResMap.get("PAGE"));
					}
				}
				
				for (int i = 0; i < DMInterURLConst.INTER_URL_LEVEL_NUM; i++) {
					if (i < strURLLevel.length)
						strInURLLevel[i] = strURLLevel[i];
					else {
						strInURLLevel[i] = DMInterURLConst.INTER_URL_NULL_LEVEL;
					}
				}
				String strUrlType = "-1";
				if (strInURLLevel.length >= DMInterURLConst.INTER_URL_FIRST_LEVEL_NUM) {
					
					for (int levelIndex = 0; levelIndex < DMInterURLConst.INTER_URL_FIRST_LEVEL_NUM; levelIndex++) {
						if (strInURLLevel[2]
								.trim()
								.equals(DMInterURLConst.INTER_URL_FIRST_LEVEL_NAME[levelIndex])) {
							int urlType = levelIndex + 1;
							strUrlType = Integer.toString(urlType);
							break;
						}
					}
					URLTypeMap.put(ConstantEnum.URL_FIRST_ID, strUrlType);
					
					if (!strUrlType.equals("5")) {
						if (strInURLLevel[3].contains("?")
								|| strInURLLevel[3].contains("-")) {
							strInURLLevel[3] = DMInterURLConst.INTER_URL_NULL_LEVEL;
						}
						if (strInURLLevel[4].contains("-")) {
							strInURLLevel[4] = DMInterURLConst.INTER_URL_NULL_LEVEL;
						}
						int codeIndex = strInURLLevel[4].indexOf("?");
						if (codeIndex >= 0) {
							strInURLLevel[4] = strInURLLevel[4].substring(0,
									codeIndex);
						}
					}
					
					
					if (this.dmInUrlMap.containsKey(strUrlType)
							&& this.dmInUrlMap.get(strUrlType).containsKey(
									strInURLLevel[3])) {
						HashMap<String, String> tempThirdmap = this.dmInUrlMap
								.get(strUrlType).get(strInURLLevel[3]);
						if (tempThirdmap.size() == 1) {
							String strSecondLevel = tempThirdmap.keySet()
									.toArray()[0].toString().trim();
							String[] strSecondkey = strSecondLevel.split("#");
							URLTypeMap.put(ConstantEnum.URL_SECOND_ID, strSecondkey[0].trim());
							if (strInURLLevel[3]
									.equals(DMInterURLConst.INTER_URL_NULL_LEVEL)) {
								URLTypeMap.put(ConstantEnum.URL_THIRD_ID,
										strSecondkey[1].trim());
							} else {
								
								String strThirdLevelName = tempThirdmap
										.values().toArray()[0].toString()
										.trim();
								if (strThirdLevelName
										.equals(DMInterURLConst.INTER_URL_NULL_LEVEL)) {
									URLTypeMap.put(ConstantEnum.URL_THIRD_ID,
											strSecondkey[1].trim());
								} else if (strThirdLevelName
										.equals(strInURLLevel[4])) {
									URLTypeMap.put(ConstantEnum.URL_THIRD_ID,
											strSecondkey[1].trim());
								}
							}
						} else {
							
							Iterator<Entry<String, String>> iter = tempThirdmap
									.entrySet().iterator();
							String strThirdKey = null;
							String strThirdVal = null;
							while (iter.hasNext()) {
								Map.Entry<String, String> entry = (Entry<String, String>) iter
										.next();
								strThirdKey = entry.getKey();
								strThirdVal = entry.getValue();
								if (strThirdVal.equals(strInURLLevel[4])) {
									URLTypeMap.put(ConstantEnum.URL_THIRD_ID,
											strThirdKey.split("#")[1].trim());
									break;
								} else if (strThirdVal.equals("n")) {
									Pattern pattern = Pattern.compile("[0-9]*");
									if (pattern
											.matcher(strInURLLevel[4].trim())
											.matches()) {
										URLTypeMap.put(ConstantEnum.URL_THIRD_ID,
												strThirdKey.split("#")[1].trim());
										break;
									}
								}
							}
							URLTypeMap.put(ConstantEnum.URL_SECOND_ID,
									strThirdKey.split("#")[0].trim());
						}
						
						if (!URLTypeMap.containsKey(ConstantEnum.URL_THIRD_ID)) {
							URLTypeMap.put(ConstantEnum.URL_THIRD_ID, "-1");
						}
					} else {
						
						URLTypeMap.put(ConstantEnum.URL_SECOND_ID, "-1");
						URLTypeMap.put(ConstantEnum.URL_THIRD_ID, "-1");
					}
				}
			} else {
				
				URLTypeMap.put(ConstantEnum.URL_FIRST_ID, "-1");
				URLTypeMap.put(ConstantEnum.URL_SECOND_ID, "-1");
				URLTypeMap.put(ConstantEnum.URL_THIRD_ID, "-1");
			}
		}
		return URLTypeMap;
	}

	public Map<String, String> getSearchExt(String URLSearchLast) {
		if (URLSearchLast.length() <= 0) {
			return null;
		}
		Map<String, String> keyAndpageMap = new WeakHashMap<String, String>();
		String strKeyWord = null;
		String strPageNum = null;

		int keyStartIndex = URLSearchLast.indexOf("word");
		int pageStartIndex = URLSearchLast.indexOf("page");
		int pageEndIndex = 0;
		int keyEndIndex = 0;
		if (URLSearchLast.contains("?") && !URLSearchLast.contains("media")) {
			keyEndIndex = URLSearchLast.indexOf("?");
		} else {
			keyEndIndex = URLSearchLast.length();
		}
		if (keyStartIndex != -1) {
			strKeyWord = URLSearchLast
					.substring(keyStartIndex + 5, keyEndIndex);
		} 
		if (pageStartIndex == -1) {
			strPageNum = new String("1");
		} else {
			
			if (URLSearchLast.charAt(pageStartIndex + 6) == '.')
				pageEndIndex = pageStartIndex + 6;
			else if (URLSearchLast.charAt(pageStartIndex + 7) == '.')
				pageEndIndex = pageStartIndex + 7;
			strPageNum = URLSearchLast.substring(pageStartIndex + 5,
					pageEndIndex);
			strPageNum = strPageNum.replaceAll("\\s", "");
			
		}
		String keyword = null;
		try {
			String tempkeyword = StringDecodeFormatUtil.decodeCodedStr(strKeyWord, "utf-8");
			keyword = tempkeyword.replaceAll("\\s", "");
		} catch (Exception e) {
			keyword = "";
			//System.out.println("关键词编解码错误URL:"+ e.toString());
			//logger.error("关键词编解码错误:" + e.getMessage(), e.getCause());
		}
		if(keyword == null){
			keyword = "";
		}
		if(strPageNum == null){
			strPageNum = "1";
		}
		
		keyAndpageMap.put("KEYWORD", keyword);
		keyAndpageMap.put("PAGE", strPageNum);
		return keyAndpageMap;
	}
	/*
	private String getMediaId(String[]  strMediaPlayURL){
		Pattern pattern = Pattern.compile("[0-9]*");
		String mediaId = "";
		for(int i = 4; i < 6; i++){
			int codeIndex = strMediaPlayURL[i].indexOf("?");
			if (codeIndex >= 0) {
				strMediaPlayURL[i] = strMediaPlayURL[i].substring(0,
						codeIndex);
			}
	   		Matcher mather = pattern.matcher(strMediaPlayURL[i].trim());
			if (mather.find()) {
				mediaId = mather.group();
			}
	    }
	   return mediaId;    
	}
	*/
}
