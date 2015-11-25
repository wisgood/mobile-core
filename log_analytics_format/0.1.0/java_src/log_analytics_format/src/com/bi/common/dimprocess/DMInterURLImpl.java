package com.bi.common.dimprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bi.common.constant.CommonConstant;
import com.bi.common.constant.DMInterURLEnum;
import com.bi.common.constant.ConstantEnum;
import com.bi.common.util.StringDecodeFormatUtil;

public class DMInterURLImpl {
	private static Logger logger = Logger.getLogger(DMInterURLImpl.class
			.getName());

	private Map<String, HashMap<String, HashMap<String, String>>> dmInUrlMap = null;

	@SuppressWarnings("unchecked")
	public void parseDMObj(File file) throws IOException {
		
		BufferedReader inFile = null;
		
		try {
			this.dmInUrlMap = new HashMap<String, HashMap<String, HashMap<String, String>>>();
			@SuppressWarnings({ "unchecked", "rawtypes" })
			
			HashMap[] levelMaps = new HashMap[CommonConstant.INTER_URL_FIRST_LEVEL_NUM];
			
			for (int i = 0; i < CommonConstant.INTER_URL_FIRST_LEVEL_NUM; i++) {
				levelMaps[i] = new HashMap<String, HashMap<String, String>>();
			}
			
			inFile = new BufferedReader(new InputStreamReader(
					new FileInputStream(file)));
			
			String line;
			
			while ((line = inFile.readLine()) != null) {
				
				String[] strURLLine = line.trim().split("\t");
				int firstIdIndex = 0;
				String[] strURL = strURLLine[DMInterURLEnum.URL.ordinal()]
						.trim().split("/");
				String strSecondName = "";
				String strThirdName = "";
				String strThird = new String();
				
				for(int i = 0; i < CommonConstant.INTER_URL_FIRST_LEVEL_NUM; i++){
					if(strURL[0].trim().equalsIgnoreCase(CommonConstant.INTER_URL_FIRST_LEVEL_NAME[i])){
						firstIdIndex = i+1;
						break;
					}
				}	
				if (strURL.length == 1) {
					strSecondName = CommonConstant.INTER_URL_NULL_LEVEL;
					strThirdName = CommonConstant.INTER_URL_NULL_LEVEL;
				} else if (strURL.length == 2) {
					strSecondName = strURL[1].trim();
					strThirdName = CommonConstant.INTER_URL_NULL_LEVEL;
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
				if (index > CommonConstant.INTER_URL_NEED_LENGTH -1)
					break;
				int mapIndex = 0;
				if(firstIdIndex > 0)
					mapIndex = firstIdIndex - 1;
				else
					mapIndex = index;
				if (levelMaps[mapIndex].containsKey(strSecondName)) {
					HashMap<String, String> thirdMap = (HashMap<String, String>) levelMaps[mapIndex]
							.get(strSecondName);
					thirdMap.put(strThird, strThirdName);	
					levelMaps[mapIndex].put(strSecondName, thirdMap);
				} else {
					HashMap<String, String> thirdNewMap = new HashMap<String, String>();
					thirdNewMap.put(strThird, strThirdName);
					levelMaps[mapIndex].put(strSecondName, thirdNewMap);
				}
			}
			for (int j = 1; j < CommonConstant.INTER_URL_FIRST_LEVEL_NUM + 1; j++) {
				this.dmInUrlMap.put(String.valueOf(j), levelMaps[j - 1]);
			}
		} catch (Exception e) {
			logger.error("站内url维度文件读取错误:" + e.getMessage(), e.getCause());
		} finally {
			inFile.close();
		}
	}

	public Map<ConstantEnum, String> getDMOjb(String param) {

		Map<ConstantEnum, String> URLTypeMap = new WeakHashMap<ConstantEnum, String>();
		
		String strInputURL = param;
		
		if (strInputURL == null || strInputURL.isEmpty()) {
			URLTypeMap.put(ConstantEnum.URL_FIRST_ID, "-1");
			URLTypeMap.put(ConstantEnum.URL_SECOND_ID, "-1");
			URLTypeMap.put(ConstantEnum.URL_THIRD_ID, "-1");
		} else {
			if (strInputURL.startsWith("http")) {				
				String[] strInURLLevel = new String[CommonConstant.INTER_URL_LEVEL_NUM];				
				String[] strURLLevel = strInputURL.trim().split("/", -1);
				if (strURLLevel.length >= 4 && strURLLevel[2].contains("so.funshion")
						&& !strURLLevel[2].contains("list")) {
					Map<String, String> searchResMap = getSearchExt(strURLLevel[strURLLevel.length - 1]);
					if (searchResMap != null) {
						URLTypeMap.put(ConstantEnum.URL_KEYWORD,
								searchResMap.get("KEYWORD"));
						URLTypeMap.put(ConstantEnum.URL_PAGE,
								searchResMap.get("PAGE"));
					}
				}
				if (strURLLevel.length >= 5 && strURLLevel[3].equals("search")
						&& !strURLLevel[4].equals("list")) {
					Map<String, String> searchResMap = getSearchExt(strURLLevel[strURLLevel.length - 1]);
					if (searchResMap != null) {
						URLTypeMap.put(ConstantEnum.URL_KEYWORD,
								searchResMap.get("KEYWORD"));
						URLTypeMap.put(ConstantEnum.URL_PAGE,
								searchResMap.get("PAGE"));
					}
				}

				for (int i = 0; i < CommonConstant.INTER_URL_LEVEL_NUM; i++) {				
				   if (i < strURLLevel.length){
						if(strURLLevel[i].isEmpty()||null == strURLLevel[i]){
							strInURLLevel[i] = CommonConstant.INTER_URL_NULL_LEVEL;
						} else if(strURLLevel[i].contains("?")){
							String[] urlFields = strURLLevel[i].split("\\?");
							if(urlFields.length > 0){
								strInURLLevel[i] = urlFields[0];
							}
						} else{
							strInURLLevel[i] = strURLLevel[i];
						}
					} else {
						strInURLLevel[i] = CommonConstant.INTER_URL_NULL_LEVEL;
					}
				}
				String urlType = "-1";
				if (strInURLLevel.length >= CommonConstant.INTER_URL_NEED_LENGTH) {
					int urlTypeId = 0;
					String urlFirst = "-1";
					for (int levelIndex = 0; levelIndex < CommonConstant.INTER_URL_FIRST_LEVEL_NUM; levelIndex++) {
						if (strInURLLevel[2]
								.trim()
								.equals(CommonConstant.INTER_URL_FIRST_LEVEL_NAME[levelIndex])) {
							urlTypeId = levelIndex + 1;
							urlFirst = Integer.toString(urlTypeId);
							if(urlTypeId > 5){
								urlType = "1";
							} else {
								urlType = Integer.toString(urlTypeId);
							}
							break;
						}
					}
					URLTypeMap.put(ConstantEnum.URL_FIRST_ID, urlType.trim());
					
					if (urlType.equals("2")||urlType.equals("3")) {
						if (strInURLLevel[3].contains("?")
								|| strInURLLevel[3].contains("-")) {
							strInURLLevel[3] = CommonConstant.INTER_URL_NULL_LEVEL;
						}
						if (strInURLLevel[4].contains("-")) {
							strInURLLevel[4] = CommonConstant.INTER_URL_NULL_LEVEL;
						}
						int codeIndex = strInURLLevel[4].indexOf("?");
						if (codeIndex > 0) {
							strInURLLevel[4] = strInURLLevel[4].substring(0,
									codeIndex);
						}
					}
					if (this.dmInUrlMap.containsKey(urlFirst)
							&& this.dmInUrlMap.get(urlFirst).containsKey(strInURLLevel[3])) {
						HashMap<String, String> tempThirdmap = this.dmInUrlMap
								.get(urlFirst).get(strInURLLevel[3]);
						
						if (tempThirdmap.size() == 1) {
							String strSecondLevel = tempThirdmap.keySet()
									.toArray()[0].toString().trim();
							String[] strSecondkey = strSecondLevel.split("#");
							URLTypeMap.put(ConstantEnum.URL_SECOND_ID,
									strSecondkey[0].trim());
							if (strInURLLevel[3]
									.equals(CommonConstant.INTER_URL_NULL_LEVEL)) {
								URLTypeMap.put(ConstantEnum.URL_THIRD_ID,
										strSecondkey[1].trim());
							} else {

								String strThirdLevelName = tempThirdmap
										.values().toArray()[0].toString()
										.trim();
								if (strThirdLevelName
										.equals(CommonConstant.INTER_URL_NULL_LEVEL)) {
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
									if (pattern.matcher(strInURLLevel[4].trim()).matches()) {
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
		if (!URLTypeMap.containsKey(ConstantEnum.URL_FIRST_ID)) {
			URLTypeMap.put(ConstantEnum.URL_FIRST_ID, "-1");
		}
		if (!URLTypeMap.containsKey(ConstantEnum.URL_SECOND_ID)) {
			URLTypeMap.put(ConstantEnum.URL_SECOND_ID, "-1");
		}
		if (!URLTypeMap.containsKey(ConstantEnum.URL_THIRD_ID)) {
			URLTypeMap.put(ConstantEnum.URL_THIRD_ID, "-1");
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

		int keyStartIndex = -1;

		int pageStartIndex = -1;

		int pageEndIndex = 0;

		int keyEndIndex = 0;

		if (URLSearchLast.contains(".")) {
			// keyEndIndex = URLSearchLast.indexOf("?");
			String[] searchFields = URLSearchLast.split("\\.", -1);
			for (String searchField : searchFields) {
				if (searchField.contains("word")) {
					keyStartIndex = searchField.indexOf("word");
					keyEndIndex = searchField.length();
					if (keyStartIndex != -1) {
						strKeyWord = searchField.substring(keyStartIndex + 5,
								keyEndIndex);
					}
				}
				pageStartIndex = searchField.indexOf("page");
				if (pageStartIndex != -1) {
					pageEndIndex = searchField.length();
					strPageNum = searchField.substring(pageStartIndex + 5,
							pageEndIndex);
				}
			}
		} else if (URLSearchLast.contains("?")
				&& !URLSearchLast.contains("media")) {
			String[] searchFields = URLSearchLast.split("\\?", -1);
			for (String searchField : searchFields) {
				if (searchField.contains("word")) {
					keyStartIndex = searchField.indexOf("word");
					keyEndIndex = searchField.length();
					if (keyStartIndex != -1) {
						strKeyWord = searchField.substring(keyStartIndex + 5,
								keyEndIndex);
					}
				}
				pageStartIndex = searchField.indexOf("page");
				if (pageStartIndex != -1) {
					pageEndIndex = searchField.length();
					strPageNum = searchField.substring(pageStartIndex + 5,
							pageEndIndex);
				}
			}
		} else {
			String[] searchFields = URLSearchLast.split("\\&", -1);
			for (String searchField : searchFields) {
				if (searchField.contains("word")) {
					keyStartIndex = searchField.indexOf("word");
					keyEndIndex = searchField.length();
					if (keyStartIndex != -1) {
						strKeyWord = searchField.substring(keyStartIndex + 5,
								keyEndIndex);
					}
				}
				pageStartIndex = searchField.indexOf("page");
				if (pageStartIndex != -1) {
					pageEndIndex = searchField.length();
					strPageNum = searchField.substring(pageStartIndex + 5,
							pageEndIndex);
				}
			}
		}

		/*
		 * if (pageStartIndex == -1) { strPageNum = new String("1"); } else { if
		 * (URLSearchLast.charAt(pageStartIndex + 6) == '.') pageEndIndex =
		 * pageStartIndex + 6; else if (URLSearchLast.charAt(pageStartIndex + 7)
		 * == '.') pageEndIndex = pageStartIndex + 7; strPageNum =
		 * URLSearchLast.substring(pageStartIndex + 5, pageEndIndex); strPageNum
		 * = strPageNum.replaceAll("\\s", "");
		 * 
		 * }
		 */
		String keyword = null;
		try {
			String tempkeyword = StringDecodeFormatUtil.decodeCodedStr(
					strKeyWord, "utf-8");
			keyword = tempkeyword.replaceAll("\\s", "");
		} catch (Exception e) {
			keyword = "";
		}
		if (keyword == null) {
			keyword = "";
		}
		if (strPageNum == null) {
			strPageNum = "1";
			strPageNum.replaceAll("\\s", "");
		}

		keyAndpageMap.put("KEYWORD", keyword);
		keyAndpageMap.put("PAGE", strPageNum);
		return keyAndpageMap;
	}
	/*
	 * private String getMediaId(String[] strMediaPlayURL){ Pattern pattern =
	 * Pattern.compile("[0-9]*"); String mediaId = ""; for(int i = 4; i < 6;
	 * i++){ int codeIndex = strMediaPlayURL[i].indexOf("?"); if (codeIndex >=
	 * 0) { strMediaPlayURL[i] = strMediaPlayURL[i].substring(0, codeIndex); }
	 * Matcher mather = pattern.matcher(strMediaPlayURL[i].trim()); if
	 * (mather.find()) { mediaId = mather.group(); } } return mediaId; }
	 */
}
