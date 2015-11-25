package com.bi.common.dimprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class DMURLListPageDAOImpl <E, T> extends AbstractDMDAO<E, T> {
	private Map<String,String> dmUrlListMap = null;
	@Override
	public void parseDMObj(File file) throws IOException {
		
		BufferedReader in = null;
		try {
			
			this.dmUrlListMap = new HashMap<String,String>();
			in = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
			String line;
			while ((line = in.readLine()) != null) {
				if (line.contains("#")) {
					continue;
				}
				String[] listTabs = line.split(",");
				if(listTabs.length > 1){
					dmUrlListMap.put(listTabs[1].trim(), listTabs[0].trim());
				}
			}
		} finally {
			in.close();
		}	
	}

	@Override
	public T getDMOjb(E param) {
		// TODO Auto-generated method stub
		//HOW TO USE DIM INFO, c-12345,c,
		
		String urlListType = "-1";
		String listTabStr = (String) param;

		String listTabType = this.dmUrlListMap.get(listTabStr.toLowerCase().trim());
		if(null != listTabType){
			urlListType = listTabType;
		}
		return (T)urlListType;
	}

}
