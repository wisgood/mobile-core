
package com.bi.ad.comm.util.format;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.io.IOUtils;

public class LiveChannelInfo {
	private ArrayList<String> liveInfo = new ArrayList<String>();
	

	public void init(String fileName) throws IOException {
		BufferedReader in = null;		
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String id   = null;
			String line = null;
			while ((line = in.readLine()) != null) {
				id = line.trim();
				liveInfo.add(id);
			}		
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}				
	}
  
	public ArrayList<String> getLiveInfo() {
		return liveInfo;
	}

	public void setLiveInfo(ArrayList<String> liveInfo) {
		this.liveInfo = liveInfo;
	}

}