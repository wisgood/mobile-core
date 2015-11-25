package com.bi.client.indexSpMouseMove.format;

import java.util.HashMap;

import com.bi.client.util.IpParser1;
import com.bi.client.util.MACFormat1;
import com.bi.client.util.TimeFormat1;

public class MousemoveLog {
	
	public void setFileds(String[] fields){
		setTimestamp(fields[MousemoveLogEnum.TIMESTAMP.ordinal()]);
		setLongIp(fields[MousemoveLogEnum.IP.ordinal()]);
		setMac(fields[MousemoveLogEnum.MAC.ordinal()]);
		setUserId(fields[MousemoveLogEnum.USER_ID.ordinal()]);
		setFpc(fields[MousemoveLogEnum.FPC.ordinal()]);
		setChannelId(fields[MousemoveLogEnum.CHANNEL_ID.ordinal()]);
		setFck(fields[MousemoveLogEnum.FCK.ordinal()]);
		setProvinceId();
		setIspId();
		setDateId();
	}
	
	//timestamp
	public String getTimestamp(){
		return timestamp;
	}
			
	public void setTimestamp(String timestamp){
		this.timestamp = timestamp;
	}
	
	//longIp
	public long getLongIp(){
		return longIp;
	}
		
	public void setLongIp(String ip){
		longIp = IpParser1.ip2long(ip);
	}
	
	//mac
	public String getMac(){
		return mac;
	}
	
	public void setMac(String amac){
		mac = MACFormat1.macFormat(amac);
	}
	
	//userId
	public String getUserId(){
		return userId;
	}
	
	public void setUserId(String aUserId){
		if(aUserId == null || "".equals(aUserId) || " ".equals(aUserId)){
			userId = "-";
		}else{
			userId = aUserId;
		}
	}	
	
	//fpc
	public String getFpc(){
		return fpc;
	}
	
	public void setFpc(String aFpc){
		if(aFpc == null || "".equals(aFpc) || " ".equals(aFpc)){
			fpc = "-";
		}else{ 
			fpc = aFpc;
		}
	}
	
	//channelId
	public String getChannelId(){
		return channelId;
	}
	
	public void setChannelId(String aChannelId){
		channelId =  channelIdMap.containsKey(aChannelId) ? aChannelId : "1";
	}
	
	//fck
	public String getFck(){
		return fck;
	}
	
	public void setFck(String aFck){
		if(aFck == null || "".equals(aFck) || " ".equals(aFck)){
			fck = "-";
		}else{
			fck = aFck;
		}
	}
	
	//provinceId
	public String  getProvinceId(){
		return provinceId;
	}
	
	public void setProvinceId(){
		provinceId = ipParser.getAreaId(longIp);
	}
	
	//ispId
	public String  getIspId(){
		return ispId;
	}
	
	public void setIspId(){
		ispId = ipParser.getIspId(longIp);
	}
	
	public void setDateId(){
		String timestampStr = TimeFormat1.toString(timestamp);
		String[] strs = timestampStr.split("\t");
		dateId = strs[1];
	}
	
	//set IpParser
	public void setIpParser(IpParser1 aIpParser){
		ipParser = aIpParser;
	}
	
	//channelId info
	public void setChannelIdMap(HashMap<String, String> aChannelIdMap){
		channelIdMap = aChannelIdMap;
	}
	
	public String toString(){
		StringBuilder str = new StringBuilder();                                                       
		String sep = "\t";
		str.append(dateId).append(sep)
		   .append(provinceId).append(sep)
		   .append(ispId).append(sep)
		   .append(mac).append(sep)
		   .append(longIp).append(sep)
		   .append(timestamp).append(sep)
		   .append(fck).append(sep)
		   .append(userId).append(sep)
		   .append(fpc).append(sep)
		   .append(channelId);
		
		return str.toString();
	}
	
	private String timestamp = null;
	private long longIp = 0;
	private String mac = null;
	private String userId = null;
	private String fpc = null;
	private String channelId = null;
	private String fck = null;
	private String provinceId = null;
	private String ispId = null;
	private String dateId = null;
	private IpParser1 ipParser = null;
	private HashMap<String, String> channelIdMap = null;
}
