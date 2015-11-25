package com.bi.common.dimprocess;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.log4j.Logger;

import com.bi.common.constant.ConstantEnum;
import com.bi.common.dm.pojo.DMURLRule;
import com.bi.common.dm.pojo.DMURLRuleArrayList;
import com.bi.common.util.IOFormatUtil;
import com.bi.common.util.StringDecodeFormatUtil;

public class DMMobileWebURLImpl<E, T> extends AbstractDMDAO<E, T> {

	private static Logger logger = Logger.getLogger(DMMobileWebURLImpl.class
			.getName());

	private DMURLRuleArrayList<DMURLRule> dmUrlRuleList = null;

	public void parseDMObj(File file) throws IOException {

		this.dmUrlRuleList = new DMURLRuleArrayList<DMURLRule>(
				new ArrayList<DMURLRule>());
		
		for (String line : IOFormatUtil.ReadLinesEx(file, "utf-8")) {
			try {
				if (line.contains("#") || line.isEmpty()) {
					continue;
				}
				String[] strPlate = line.split("\t", -1);
				DMURLRule dmURLRule = new DMURLRule(
						Integer.parseInt(strPlate[0]), strPlate[1],
						Integer.parseInt(strPlate[2]), strPlate[3],
						Integer.parseInt(strPlate[4]), strPlate[5],
						strPlate[6], Integer.parseInt(strPlate[7]), strPlate[8]);
				if (null != dmURLRule) {
					dmUrlRuleList.add(dmURLRule);
				}
			} catch (Exception e) {
				logger.error("移动url输入错误:" + e.getMessage(), e.getCause());
				continue;
			}
		}
	}

	public T getDMOjb(E param) throws Exception {

		Map<ConstantEnum, String> urlRuleMap = new WeakHashMap<ConstantEnum, String>();
		String url = (String) param;

		DMURLRule dmURLRule = this.dmUrlRuleList.getDmURLRule(url);
		if (null == dmURLRule) {
			urlRuleMap.put(ConstantEnum.URL_FIRST_ID, "-999");
			urlRuleMap.put(ConstantEnum.URL_SECOND_ID, "-999");
			urlRuleMap.put(ConstantEnum.URL_THIRD_ID, "-999");
		} else {
			urlRuleMap.put(ConstantEnum.URL_FIRST_ID, dmURLRule.getFirstId()
					+ "");
			urlRuleMap.put(ConstantEnum.URL_SECOND_ID, dmURLRule.getSecondId()
					+ "");
			urlRuleMap.put(ConstantEnum.URL_THIRD_ID, dmURLRule.getThirdId()
					+ "");
		}
		if(url.contains("search")){
			Map<String, String> keywordMap = getSearchExt(url.trim());
		
			if(keywordMap.size() >= 2){
				urlRuleMap.put(ConstantEnum.URL_KEYWORD, keywordMap.get("KEYWORD")
						+ "");
				urlRuleMap.put(ConstantEnum.URL_PAGE, keywordMap.get("PAGE")
						+ "");
			}
		}
		return (T) urlRuleMap;
	}
	
	public Map<String, String> getSearchExt(String URLSearchLast)throws Exception {
		
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
		
		String[] searchFields = null;
		
		if (URLSearchLast.contains("?")
				&& !URLSearchLast.contains("media")) {
			searchFields = URLSearchLast.split("\\?", -1);
		} else {
			searchFields = URLSearchLast.split("\\&", -1);
		}
		for (String searchField : searchFields) {
			if (searchField.contains("word")) {
				keyStartIndex = searchField.indexOf("word");
				keyEndIndex = searchField.length();
				if (keyStartIndex != -1 && keyStartIndex + 5 < keyEndIndex) {
					strKeyWord = searchField.substring(keyStartIndex + 5,
							keyEndIndex);
				}
			}
			pageStartIndex = searchField.indexOf("page");
			if (pageStartIndex != -1) {
				pageEndIndex = searchField.length();
				if(pageStartIndex + 5 < pageEndIndex){
					strPageNum = searchField.substring(pageStartIndex + 5,
							pageEndIndex);
				}
			}
		}
		String keyword = null;
		try {
			String tempkeyword = StringDecodeFormatUtil.decodeCodedStr(
					strKeyWord, "utf-8");
			keyword = tempkeyword.replaceAll("\\s+", "");
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
}
