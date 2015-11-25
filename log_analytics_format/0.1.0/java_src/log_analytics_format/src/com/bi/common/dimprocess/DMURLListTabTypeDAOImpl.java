package com.bi.common.dimprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class DMURLListTabTypeDAOImpl <E, T> extends AbstractDMDAO<E, T> {
	
	private static final String FIELD_SPLIT_COMM = ",";
	
	private static final String FIELD_SPLIT_SPE = "#";

	private Map<String, String> dmUrlListTabMap = null;

	public void parseDMObj(File file) throws IOException {

		BufferedReader in = null;
		try {
			this.dmUrlListTabMap = new HashMap<String, String>();
			// this.dmUrlListTypeMap = new HashMap<String,String>();
			in = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
			String line;
			while ((line = in.readLine()) != null) {
				String[] listFields = line.split(FIELD_SPLIT_SPE);
				if (listFields.length > 2) {
					// dmUrlListTypeMap.put(listFields[1].trim(),
					// listFields[0].trim());
					
					dmUrlListTabMap.put(listFields[1].trim(),
							listFields[2].trim());
				}
			}
		} finally {
			in.close();
		}
	}

	@Override
	public T getDMOjb(E param) {
		// HOW TO USE DIM INFO, c-12345,c
		
		String listStr = (String) param;
		String listTabList = "-1";

		Iterator<Entry<String, String>> iter = this.dmUrlListTabMap.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = (Entry<String, String>) iter
					.next();
			String key = entry.getKey();
			String[] urlTypes = key.split(FIELD_SPLIT_COMM);
			for (String urltype : urlTypes) {
				if (urltype.trim().equals(listStr.trim())) {
					listTabList = entry.getValue();
					break;
				}
			}
		}
		return (T) listTabList.trim();
	}
}
