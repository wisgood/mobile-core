package com.bi.common.dm.pojo.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class DMQuDaoRuleDAOImpl<T, E> extends AbstractDMDAO<E, T> {

	private List<Integer> dmQuDaoInfoList = null;

	@Override
	public void parseDMObj(File file) throws IOException {
		// TODO Auto-generated method stub
		
		BufferedReader in = null;
		try {
			this.dmQuDaoInfoList = new ArrayList<Integer>();
			in = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
			String line;
			while ((line = in.readLine()) != null) {
				if (line.contains("#")) {
					continue;
				}
				String[] strPlate = line.split("\t");
				Integer qudiaoId = Integer.valueOf(strPlate[0]);
				this.dmQuDaoInfoList.add(qudiaoId);
			}
		} finally {
			in.close();
		}
	}

	@Override
	public T getDMOjb(E param) {
		// TODO Auto-generated method stub
		Integer qudaoId = (Integer) param;
		if (this.dmQuDaoInfoList != null && this.dmQuDaoInfoList.contains(qudaoId)) {
			return (T) qudaoId;
		} else {
			return (T) new Integer(1);
		}
	
	}

}
