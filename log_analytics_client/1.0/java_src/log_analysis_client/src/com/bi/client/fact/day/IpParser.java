package com.bi.client.fact.day;

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
			String areaIdStr = "99"; // 其他
			String ispStr    = "9";  // 未知
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
	
	public static String longtoip(long ip)
	 {
	  StringBuffer sb=new StringBuffer("");
	  sb.append(String.valueOf(ip>>>24));
	  sb.append(".");
	  sb.append(String.valueOf((ip&0x00FFFFFF)>>>16));
	  sb.append(".");
	  sb.append(String.valueOf((ip&0x0000FFFF)>>>8));
	  sb.append(".");
	  sb.append(String.valueOf((ip&0x000000FF)));
	  return sb.toString();
	  
	 }
	

	public String getAreaId(long longIP) {		
	    String areaId = "99";
		if (treeMap.floorEntry(longIP) != null) {
			long tmpKey = treeMap.floorKey(longIP);    			
			if (hashMap.containsKey(tmpKey) && longIP <= hashMap.get(tmpKey))
				areaId = treeMap.get(tmpKey).get(0);
		}		
		return areaId;
	}
	
    public String getIspId(long longIP) {        
        String IspId = "9";
        if (treeMap.floorEntry(longIP) != null) {
            long tmpKey = treeMap.floorKey(longIP);               
            if (hashMap.containsKey(tmpKey) && longIP <= hashMap.get(tmpKey))
                IspId = treeMap.get(tmpKey).get(1);
        }       
        return IspId;
    }
}