package com.bi.client.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.hadoop.io.IOUtils;


public class IpParser {
	TreeMap<Long, ArrayList<String>> treeMap = new TreeMap<Long,ArrayList<String>>();
	HashMap<Long, Long> hashMap = new HashMap<Long, Long>();

	public void init(String fileName) throws IOException {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String line;
			String areaIdStr = "-1"; // 其他
			String ispStr    = "-1";  // 未知
			while ((line = in.readLine()) != null) {
				String[] fields = line.trim().split("\t");
				long startIp   = Long.parseLong(fields[0]);
				long endIp     = Long.parseLong(fields[1]);
				if(fields.length == 5){
				    areaIdStr  = fields[2]; 
				    ispStr     = fields[4];
				}

				ArrayList<String> tmp = new ArrayList<String>(2);
				tmp.add(areaIdStr);
				tmp.add(ispStr);
				treeMap.put(startIp, tmp);     // treeMap will contains start_ip => [area_id,isp_id]
				hashMap.put(startIp, endIp);    // hashMap will contains start_ip => area_id
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
	
	   public static long version2long(String str) {
	        if (str == null)
	            return 0;
	        String newStr = str.toLowerCase().replace("beta","");
	        if (!newStr.matches("^\\d+\\.\\d+\\.\\d+\\.\\d+$"))
	            return 0;
	        long[] nFields = new long[4];
	        String[] strFields = newStr.split("\\.");
	        for (int i = 0; i < 4; i++) {
	            nFields[i] = Integer.parseInt(strFields[i]);
	        }
	        return (nFields[0] << 24) + (nFields[1] << 16) + (nFields[2] << 8)
	                + nFields[3];
	    }
	

	public String getAreaId(long longIP) {		
	    String areaId = "-1";
		if (treeMap.floorEntry(longIP) != null) {
			long tmpKey = treeMap.floorKey(longIP);    			
			if (hashMap.containsKey(tmpKey) && longIP <= hashMap.get(tmpKey))
				areaId = treeMap.get(tmpKey).get(0);
		}		
		return areaId;
	}
	
    public String getIspId(long longIP) {        
        String IspId = "-1";
        if (treeMap.floorEntry(longIP) != null) {
            long tmpKey = treeMap.floorKey(longIP);               
            if (hashMap.containsKey(tmpKey) && longIP <= hashMap.get(tmpKey))
                IspId = treeMap.get(tmpKey).get(1);
        }       
        return IspId;
    }
    
    public static void main(String[]  args){
        System.out.print(version2long("2.8.6.68Beta"));
    }
}