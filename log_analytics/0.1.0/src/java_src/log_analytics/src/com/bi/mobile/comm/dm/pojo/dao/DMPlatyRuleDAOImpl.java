package com.bi.mobile.comm.dm.pojo.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;


public class DMPlatyRuleDAOImpl<E, T> extends AbstractDMDAO<E, T> {
	private static Logger logger = Logger.getLogger(DMPlatyRuleDAOImpl.class
			.getName());
	private Map<String, Integer> dmDevRuleMap = null;
	@Override
	public void parseDMObj(File file) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader in = null;
		try {
			this.dmDevRuleMap = new HashMap<String, Integer>();
			in = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
			String line = null;
			while ((line = in.readLine()) != null) {
				if (line.contains("#")) {
					continue;
				}
				String[] strPlate = line.split("\t");
				this.dmDevRuleMap.put(strPlate[0].toLowerCase(),
						Integer.valueOf(strPlate[1]));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logger.error("设备类型格式不对:"+e.getMessage(),e.getCause());
			throw (IOException)e;
		}
		
		finally {
			in.close();
		}
	}

	@Override
	public T getDMOjb(E param) throws Exception {
		// TODO Auto-generated method stub
		Integer platId = 7;
		String platyInfo = (String) param;
		try {
			platId = this.dmDevRuleMap.get(platyInfo);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			throw e;
		}
		return (T) platId;
	}

}
