
package com.bi.ad.comm.util.format;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.io.IOUtils;

public class MatInfo {
	private HashMap<String, Integer> matInfo= new HashMap<String, Integer>();	
	public void init(String fileName) throws IOException {
		BufferedReader in = null;		
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String line = null;
			String materialID = null;
			String playDur = null;
			while ((line = in.readLine()) != null) {
				String fields[] = line.trim().split("\t");
				if (fields.length == 2) {
				    materialID = fields[0];
				    playDur = fields[1];
				    // Original playDur is in second, need to turn into ms when putting into HashMap
				    matInfo.put(materialID, new Integer(Integer.parseInt(playDur) * 1000));
				}
			}			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}		
	}
	
	public HashMap<String, Integer> getMatInfo() {
		return matInfo;
	}
	public void setMatInfo(HashMap<String, Integer> matInfo) {
		this.matInfo = matInfo;
	}
	
}