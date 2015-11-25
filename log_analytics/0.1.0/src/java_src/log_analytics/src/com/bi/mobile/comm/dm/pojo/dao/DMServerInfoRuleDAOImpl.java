package com.bi.mobile.comm.dm.pojo.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.WeakHashMap;

import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.DMServerInfoRule;

public class DMServerInfoRuleDAOImpl<E,T > extends AbstractDMDAO<E, T> {
private Map<String, DMServerInfoRule> dmServerInfoRuleMap = null;
	@Override
	public void parseDMObj(File file) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader in = null;
		try {
			this.dmServerInfoRuleMap = new WeakHashMap<String, DMServerInfoRule>();
			in = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
			String line;
			while ((line = in.readLine()) != null) {
				if (line.contains("#")) {
					continue;
				}
				String[] strPlate = line.split("\t");
				DMServerInfoRule tmpDMServerInfoRule = new DMServerInfoRule(
						Long.parseLong(strPlate[0]), strPlate[1], strPlate[2],
						strPlate[3]);
				dmServerInfoRuleMap.put(strPlate[0], tmpDMServerInfoRule);
			}
		} finally {
			in.close();
		}
	}

	@Override
	public T getDMOjb(E param) {
		// TODO Auto-generated method stub
		Map<ConstantEnum, String> serverInfoMap = new WeakHashMap<ConstantEnum, String>();
		String serverIPLongStr = (String) param;
		DMServerInfoRule tmpDMServerInfoRule =this.dmServerInfoRuleMap.get(serverIPLongStr);
		if (null == tmpDMServerInfoRule) {
			serverInfoMap.put(ConstantEnum.SERVER_ID, "-1");
			serverInfoMap.put(ConstantEnum.SERVERROOM_ID, "-1");
			serverInfoMap.put(ConstantEnum.SERVERROOM_NAME, "-1");
		} else {
			serverInfoMap.put(ConstantEnum.SERVER_ID, serverIPLongStr);
			serverInfoMap.put(ConstantEnum.SERVERROOM_ID,
					tmpDMServerInfoRule.getServerEngineRoomId());
			serverInfoMap.put(ConstantEnum.SERVERROOM_NAME,
					tmpDMServerInfoRule.getServerEngineRoomName());
		}
		return (T) serverInfoMap;
	}

}
