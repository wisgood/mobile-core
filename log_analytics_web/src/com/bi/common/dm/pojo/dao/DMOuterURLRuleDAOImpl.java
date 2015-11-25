package com.bi.common.dm.pojo.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.bi.common.dm.pojo.DMURLRule;
import com.bi.common.init.ConstantEnum;
import com.bi.common.util.DMURLRuleArrayList;


public class DMOuterURLRuleDAOImpl<E, T> extends AbstractDMDAO<E, T>  {
    
	 private static Logger logger = Logger.getLogger(DMOuterURLRuleDAOImpl.class
			.getName());
	 private DMURLRuleArrayList<DMURLRule> dmUrlRuleList = null;
	 
	 public static List<String> ReadLinesEx(File file, String fileCoding) { 
         ArrayList<String> lineList = new ArrayList<String>();
         try { 
             FileInputStream fis = new FileInputStream(file);
             BufferedReader br = new BufferedReader(new InputStreamReader(fis, fileCoding));
             String line = null;
              while (( line = br.readLine()) != null)
              {
                  lineList.add(line);
              }
              fis.close();
              br.close();
         } catch (IOException e) {
             e.printStackTrace(); 
             return null; 
         } 
         
         return lineList; 
     } 
	 
	 
	 //加载URL维度表
	 public void parseDMObj(File file) throws IOException {
         
		 this.dmUrlRuleList = new DMURLRuleArrayList<DMURLRule>(new ArrayList<DMURLRule>());
		 for(String line:ReadLinesEx(file, "utf-8"))
	    {	
			 try{
			      String[] strPlate = line.split("\t");
			      DMURLRule dmURLRule = new DMURLRule(Integer.parseInt(strPlate[0]),
						strPlate[1],
						Integer.parseInt(strPlate[2]),
						strPlate[3],
						Integer.parseInt(strPlate[4]),
						strPlate[5],
						strPlate[6],
						Integer.parseInt(strPlate[7]),
						strPlate[8]);
				  if(null  != dmURLRule){
					   dmUrlRuleList.add(dmURLRule);
				}
			}catch(Exception e){
					//System.out.println("error ipstr:"+strPlate);
					//e.printStackTrace();
				  logger.error("url格式不对:"+e.getMessage(), e.getCause());
			      continue;
		     }
	    }
	 }

	 //获取URL的一级分类，二级分类等信息
	@Override
	public T getDMOjb(E param) throws Exception {
		// TODO Auto-generated method stub
		Map<ConstantEnum, String> urlRuleMap = new WeakHashMap<ConstantEnum, String>();
		String url = (String)param;
		DMURLRule dmURLRule = this.dmUrlRuleList.getDmURLRule(url);
		if (null == dmURLRule) {
			urlRuleMap.put(ConstantEnum.URL_FIRST_ID, "-1");
			urlRuleMap.put(ConstantEnum.URL_SECOND_ID, "-1");
			urlRuleMap.put(ConstantEnum.URL_THIRD_ID, "-1");
		} else {
			urlRuleMap.put(ConstantEnum.URL_FIRST_ID, dmURLRule.getFirstId() + "");
			urlRuleMap.put(ConstantEnum.URL_SECOND_ID, dmURLRule.getSecondId() + "");
			urlRuleMap.put(ConstantEnum.URL_THIRD_ID, dmURLRule.getThirdId() + "");
		}
		return (T) urlRuleMap;
	}


}
