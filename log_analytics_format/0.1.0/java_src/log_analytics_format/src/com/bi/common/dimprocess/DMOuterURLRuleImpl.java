package com.bi.common.dimprocess;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.log4j.Logger;

import com.bi.common.dm.pojo.DMURLRule;
import com.bi.common.dm.pojo.DMURLRuleArrayList;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.util.IOFormatUtil;
import com.bi.common.constant.ConstantEnum;

public class DMOuterURLRuleImpl<E, T> extends AbstractDMDAO<E, T> {

	private static Logger logger = Logger.getLogger(DMOuterURLRuleImpl.class
			.getName());
	private DMURLRuleArrayList<DMURLRule> dmUrlRuleList = null;

	// 加载URL维度表
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
				// System.out.println("error ipstr:"+strPlate);
				// e.printStackTrace();
				logger.error("url格式不对:" + e.getMessage(), e.getCause());
				continue;
			}
		}
	}

	// 获取URL的一级分类，二级分类等信息
	@Override
	public T getDMOjb(E param) throws Exception {
		// TODO Auto-generated method stub
		Map<ConstantEnum, String> urlRuleMap = new WeakHashMap<ConstantEnum, String>();
		String url = (String) param;
		if (url == null || "".equals(url)) {
			urlRuleMap.put(ConstantEnum.URL_FIRST_ID, "1007");
			urlRuleMap.put(ConstantEnum.URL_SECOND_ID, "1041");
			urlRuleMap.put(ConstantEnum.URL_THIRD_ID, "1149");
			return (T) urlRuleMap;
		}
		DMURLRule dmURLRule = this.dmUrlRuleList.getDmURLRule(url);
		if (null == dmURLRule) {
			urlRuleMap.put(ConstantEnum.URL_FIRST_ID, "-1");
			urlRuleMap.put(ConstantEnum.URL_SECOND_ID, "-1");
			urlRuleMap.put(ConstantEnum.URL_THIRD_ID, "-1");

		} else if (dmURLRule.getFirstId() == 1007) {

			urlRuleMap.put(ConstantEnum.URL_FIRST_ID, "1008");
			urlRuleMap.put(ConstantEnum.URL_SECOND_ID, "1042");
			urlRuleMap.put(ConstantEnum.URL_THIRD_ID, "1150");
		} else {
			urlRuleMap.put(ConstantEnum.URL_FIRST_ID, dmURLRule.getFirstId()
					+ "");
			urlRuleMap.put(ConstantEnum.URL_SECOND_ID, dmURLRule.getSecondId()
					+ "");
			urlRuleMap.put(ConstantEnum.URL_THIRD_ID, dmURLRule.getThirdId()
					+ "");
		}
		return (T) urlRuleMap;
	}

}
