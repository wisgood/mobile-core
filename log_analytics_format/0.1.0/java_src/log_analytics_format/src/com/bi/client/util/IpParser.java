package com.bi.client.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.hadoop.io.IOUtils;

import com.bi.client.constantEnum.ConstantEnum;


public class IpParser {
	TreeMap<Long, HashMap<ConstantEnum,String>> treeMap = new TreeMap<Long,HashMap<ConstantEnum,String>>();
	HashMap<Long, Long> hashMap = new HashMap<Long, Long>();
	private final String DEFAULT_PROVINCE_ID = "-999";
	private final String DEFAULT_CITY_ID = "-999";
	private final String DEFAULT_ISP_ID = "-999";
	private final HashMap<ConstantEnum, String> DEFAULT_AREA_MAP = new HashMap<ConstantEnum, String>(){{
		put(ConstantEnum.PROVINCE_ID,DEFAULT_PROVINCE_ID);
		put(ConstantEnum.CITY_ID,DEFAULT_CITY_ID);
		put(ConstantEnum.ISP_ID,DEFAULT_ISP_ID);
	}};

	public void init(String fileName) throws IOException {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String line;
			String provinceId = "-999"; // 其他
			String cityId = "-999";
			String ispId    = "-999";  // 未知
			while ((line = in.readLine()) != null) {
				String[] fields = line.trim().split("\t");
				long startIp   = Long.parseLong(fields[0]);
				long endIp     = Long.parseLong(fields[1]);
				if(fields.length == 5){
				    provinceId  = fields[2];
				    cityId = fields[3];
				    ispId     = fields[4];
				}

				HashMap<ConstantEnum,String> areaMap = new HashMap<ConstantEnum,String>();
				areaMap.put(ConstantEnum.PROVINCE_ID,provinceId);
				areaMap.put(ConstantEnum.CITY_ID,cityId);
				areaMap.put(ConstantEnum.ISP_ID,ispId);
				treeMap.put(startIp, areaMap);     // treeMap will contains start_ip => [area_id,isp_id]
				hashMap.put(startIp, endIp);    // hashMap will contains start_ip => area_id
			}
		} finally {
			IOUtils.closeStream(in);
		}
	}
	
	public static long ip2long(String str) {
		str = ipVerify(str);
		long[] nFields = new long[4];
		String[] strFields = str.split("\\.");
		for (int i = 0; i < 4; i++) {
			nFields[i] = Integer.parseInt(strFields[i]);
		}
		return (nFields[0] << 24) + (nFields[1] << 16) + (nFields[2] << 8)
				+ nFields[3];
	}
	
	public static String ipVerify(String str){
		String retIp = "0.0.0.0";
		if (str != null && str.matches("^\\d+\\.\\d+\\.\\d+\\.\\d+$")){
			retIp = str;
		}
		return retIp;
	}
	

	public HashMap<ConstantEnum, String> getAreaMap(long longIP) {		
	    HashMap<ConstantEnum, String> ret = DEFAULT_AREA_MAP;
		if (treeMap.floorEntry(longIP) != null) {
			long tmpKey = treeMap.floorKey(longIP);    			
			if (hashMap.containsKey(tmpKey) && longIP <= hashMap.get(tmpKey))
				ret = treeMap.get(tmpKey);
		}		
		return ret;
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
}