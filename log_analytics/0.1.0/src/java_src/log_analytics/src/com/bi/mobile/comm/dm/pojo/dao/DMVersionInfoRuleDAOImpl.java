package com.bi.mobile.comm.dm.pojo.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.WeakHashMap;

import com.bi.mobile.comm.dm.pojo.DMVersionInfoRule;

public class DMVersionInfoRuleDAOImpl<E, T> extends AbstractDMDAO<E, T> {
  private Map<String, DMVersionInfoRule> dwVersionInfoRuleMap = null;
	@Override
	public void parseDMObj(File file) throws IOException {
		// TODO Auto-generated method stub
		
		BufferedReader in = null;
		try {
			this.dwVersionInfoRuleMap = new WeakHashMap<String, DMVersionInfoRule>();
			in = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
			String line;
			while ((line = in.readLine()) != null) {
				if (line.contains("#")) {
					continue;
				}
				String[] strPlate = line.split("\t");
				DMVersionInfoRule tmpDMVersionInfoRule = new DMVersionInfoRule(
						Long.parseLong(strPlate[0]), strPlate[1]);
				this.dwVersionInfoRuleMap .put(strPlate[0], tmpDMVersionInfoRule);
			}
		} finally {
			in.close();
		}
	}

	@Override
	public T getDMOjb(E param) throws Exception{
		// TODO Auto-generated method stub
		Long versionLong = -1L;
		String versionInfo = (String) param;
		try {
			versionLong = this.dwVersionInfoRuleMap.get(versionInfo).getVersionLong();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			throw e;
		}
		return (T) versionLong;
	}

}
