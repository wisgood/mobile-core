
package com.bi.ad.comm.util.format;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.io.IOUtils;

public class AdpInfo {
	private HashMap<String, AdpBase> adpInfo = new HashMap<String, AdpBase>();
	public void init(String fileName) throws IOException {
		BufferedReader in = null;		
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String line = null;
			String code = null;
			while ((line = in.readLine()) != null) {
				String[] fields = line.trim().split("\t");
				code 	= fields[1];
				AdpBase adpBase = new AdpBase(fields);
				adpInfo.put(code, adpBase);
			}		
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}				
	}
	public HashMap<String, AdpBase> getAdpInfo() {
		return adpInfo;
	}
	public void setAdpInfo(HashMap<String, AdpBase> adpInfo) {
		this.adpInfo = adpInfo;
	}	
	
}