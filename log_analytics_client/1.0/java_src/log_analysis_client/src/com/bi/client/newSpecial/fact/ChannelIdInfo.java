package com.bi.client.fact.newSpecial;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.io.IOUtils;

public class ChannelIdInfo {
	HashMap<String, String> channelIdMap = new HashMap<String, String>();
	
	public void init(String fileName) throws IOException{
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String channelId = null;
			String parentChannelId = null;
			String line = null;
			while( (line = in.readLine()) != null){
				String[] fields = line.toString().split("\t");
				channelId = fields[0];
				parentChannelId = fields[1];
				channelIdMap.put(channelId, parentChannelId);
			}
		}finally{
			IOUtils.closeStream(in);
		}
	}
	
	public HashMap<String, String> getChannelIdMap(){
		return channelIdMap;
	}
	
	public void setChannelIdSet(HashMap<String, String> channelIdMap){
		this.channelIdMap = channelIdMap;
	}
}
