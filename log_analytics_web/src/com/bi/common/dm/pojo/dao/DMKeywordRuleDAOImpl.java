package com.bi.common.dm.pojo.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.log4j.Logger;

import com.bi.common.util.DMKeywordRuleArrayList;
import com.bi.common.init.ConstantEnum;
import com.bi.common.dm.pojo.DMKeywordRule;

public class DMKeywordRuleDAOImpl<E, T> extends AbstractDMDAO<E, T> {
	
	private static Logger logger = Logger.getLogger(DMKeywordRuleDAOImpl.class
			.getName());
	private DMKeywordRuleArrayList<DMKeywordRule> DMKeywordRuleList = null;
	
	
	/** 
	 *desc： 加载 URL_keywordTemplate 维度表
	 *param：
	 *     file: URL_keywordTemplate 对应维度表      
	 *ret:
	 *     DMKeywordRuleList: List<DMKeywordRule>
	 **/
	@Override
	public void parseDMObj(File file) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader 	in = null;
		try {
			this.DMKeywordRuleList = new DMKeywordRuleArrayList<DMKeywordRule>(new ArrayList<DMKeywordRule>());
			in = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
			String line;
			while ((line = in.readLine()) != null) {
				if (line.contains("#")) {
					continue;
				}
				String[] strPlate = line.split("\t");
				if (this.isContainsEmptyStrs(strPlate)) {
					continue;
				}
				try{				
				DMKeywordRule DMKeywordRule = new DMKeywordRule(strPlate[0],strPlate[1]);
				if(null  != DMKeywordRule){
					DMKeywordRuleList.add(DMKeywordRule);
				}
				}catch(Exception e){
					//System.out.println("error ipstr:"+strPlate);
					//e.printStackTrace();
					logger.error("url_keywordTemplate格式不对:"+e.getMessage(), e.getCause());
					 continue;
				}			
			}
		} finally {
			in.close();
		}		
	}
	
	
	 /**
	 *desc：根据URL获取keyword
	 *param：
	 *     url      
	 *ret:
	 *     keywordRuleMap:
	 **/
	
	@Override
	public T getDMOjb(E param) {
		// TODO Auto-generated method stub
		Map<ConstantEnum, String> keywordRuleMap = new WeakHashMap<ConstantEnum, String>();
		String url = (String) param;
		String keyword = this.DMKeywordRuleList.getDmKeywordRule(url);
		if (null == keyword || keyword == "") {
			keywordRuleMap.put(ConstantEnum.URL_KEYWORD, "");
		} else {
			keywordRuleMap.put(ConstantEnum.URL_KEYWORD, keyword + "");
		}
		return (T) keywordRuleMap;
	}

}
