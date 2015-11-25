package com.bi.common.etl.pojo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import com.bi.common.etl.constant.ConstantEnum;
/**
 * hash大写
 * @author fysyihui
 *
 * @param <E>
 * @param <T>
 */
public class DMInfoHashRuleDAOImpl<E, T> extends AbstractDMDAO<E, T> {
	private Map<String,DMInfoHashRule> dmInforHashRuleMap =null;
	@Override
	public void parseDMObj(File file) throws IOException {
		BufferedReader in = null;
		try {
			
			this.dmInforHashRuleMap = new HashMap<String,DMInfoHashRule>();
			in = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
			String line;
			while ((line = in.readLine()) != null) {
				if (line.contains("#")) {
					continue;
				}
				String[] strPlate = line.split("\t");
				DMInfoHashRule tmpDMInforHashRule =new DMInfoHashRule(strPlate[0].toLowerCase(),strPlate[1],Long.parseLong(strPlate[2]),Long.parseLong(strPlate[3]));
				dmInforHashRuleMap.put(strPlate[0].toUpperCase(), tmpDMInforHashRule);
			}
		} finally {
			in.close();
		}	
	}

	@Override
	public T getDMOjb(E param) {
		Map<ConstantEnum,String> inforHashMap = new HashMap<ConstantEnum,String>();
		String infoHashStr = (String) param;
		//System.out.println("hashinfo:"+infoHashStr.toLowerCase());
		DMInfoHashRule tmpDMInforHashRule = this.dmInforHashRuleMap.get(infoHashStr.toLowerCase());
		if(null == tmpDMInforHashRule){
			inforHashMap.put(ConstantEnum.CHANNEL_ID, "-1");
			inforHashMap.put(ConstantEnum.SERIAL_ID, "-1");
			inforHashMap.put(ConstantEnum.MEIDA_ID, "-1");
		}else {
		inforHashMap.put(ConstantEnum.CHANNEL_ID, tmpDMInforHashRule.getChannelId()+"");
		inforHashMap.put(ConstantEnum.SERIAL_ID, tmpDMInforHashRule.getSerialId());
		inforHashMap.put(ConstantEnum.MEIDA_ID, tmpDMInforHashRule.getMediaId()+"");
		}
		return (T) inforHashMap;
	}

}
