package com.bi.ad.comm.util.format;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.hadoop.io.IOUtils;

public class IpArea {
	TreeMap<Long, String> treeMap = new TreeMap<Long, String>();
	HashMap<Long, Long> hashMap = new HashMap<Long, Long>();
	final String DefaultAreaId = "99";

	public void init(String fileName) throws IOException {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String line;
			while ((line = in.readLine()) != null) {
				String fields[] = line.trim().split("\t");
				long startIp   = Long.parseLong(fields[0]);
				long endIp     = Long.parseLong(fields[1]);
				String areaId  = fields[2];   
				treeMap.put(startIp, areaId);     // treeMap will contains start_ip => area_id
				hashMap.put(startIp, endIp);      // hashMap will contains start_ip => area_id
			}
		} finally {
			IOUtils.closeStream(in);
		}
	}
	
	public static long ip2long(String str) {
		if (str == null)
			return 0;
		if (!str.matches("^\\d+\\.\\d+\\.\\d+\\.\\d+$"))
			return 0;
		long[] nFields = new long[4];
		String[] strFields = str.split("\\.");
		for (int i = 0; i < 4; i++) {
			nFields[i] = Integer.parseInt(strFields[i]);
		}
		return (nFields[0] << 24) + (nFields[1] << 16) + (nFields[2] << 8)
				+ nFields[3];
	}
	

	public String getAreaID(long longIP) {
		String areaID = DefaultAreaId;
		
		if (treeMap.floorEntry(longIP) != null) {
			// floorKey returns the greatest key less than or equal to the given key, or null if there is no such key.
			// after treeMap.floorKey(longIP), tmpKey will contain the greatest start_ip less than or equal to the given key.
			long tmpKey = treeMap.floorKey(longIP);    
			
			// if longIP >= start_ip && longIP <= end_ip, return relevant areaID
			if (hashMap.containsKey(tmpKey) && longIP <= hashMap.get(tmpKey))
				areaID = treeMap.get(tmpKey);
		}		
		return areaID;
	}

	public TreeMap<Long, String> getTypeName() {
		return treeMap;
	}

	public HashMap<Long, Long> getIpregion() {
		return hashMap;
	}

}