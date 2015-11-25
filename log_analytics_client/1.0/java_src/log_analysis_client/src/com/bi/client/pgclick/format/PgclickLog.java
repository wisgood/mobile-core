package com.bi.client.pgclick.format;

import java.util.HashMap;

import com.bi.client.util.IpParser1;
import com.bi.client.util.MACFormat1;
import com.bi.client.util.TimeFormat1;

public class PgclickLog {
	
	public void setFields(String[] fields){
		setProtocol(fields[PgclickLogEnum.PROTOCOL.ordinal()]);
		setRprotocol(fields[PgclickLogEnum.RPROTOCOL.ordinal()]);
		setClientFlag(fields[PgclickLogEnum.CLIENT_FLAG.ordinal()]);
		setTimestamp(fields[PgclickLogEnum.TIMESTAMP.ordinal()]);
		setLongIp(fields[PgclickLogEnum.IP.ordinal()]);
		setFck(fields[PgclickLogEnum.FCK.ordinal()]);
		setMac(fields[PgclickLogEnum.MAC.ordinal()]);
		setUserId(fields[PgclickLogEnum.USER_ID.ordinal()]);
		setFpc(fields[PgclickLogEnum.FPC.ordinal()]);
		setVersionId(fields[PgclickLogEnum.VERSION.ordinal()]);
		setSid(fields[PgclickLogEnum.SID.ordinal()]);
		setPvid(fields[PgclickLogEnum.PVID.ordinal()]);
		setConfig(fields[PgclickLogEnum.CONFIG.ordinal()]);
		setUrl(fields[PgclickLogEnum.URL.ordinal()]);
		setReferUrl(fields[PgclickLogEnum.REFERURL.ordinal()]);
		setChannelId(fields[PgclickLogEnum.CHANNEL_ID.ordinal()]);
		setBlock(fields[PgclickLogEnum.BLOCK.ordinal()]);
		setScreenWidth(fields[PgclickLogEnum.SCREENW.ordinal()]);
		setScreenHeight(fields[PgclickLogEnum.SCREENH.ordinal()]);
		setBrowserWidth(fields[PgclickLogEnum.BROWSERW.ordinal()]);
		setBrowserHeight(fields[PgclickLogEnum.BROWSERH.ordinal()]);
		setBrowserPx(fields[PgclickLogEnum.BROWSERPX.ordinal()]);
		setBrowserPy(fields[PgclickLogEnum.BROWSERPY.ordinal()]);
		setPagePx(fields[PgclickLogEnum.PAGEPX.ordinal()]);
		setPagePy(fields[PgclickLogEnum.PAGEPY.ordinal()]);
		setExt(fields[PgclickLogEnum.EXT.ordinal()]);
		setUserAgent(fields[PgclickLogEnum.USERAGENT.ordinal()]);
		setProvinceId();
		setIspId();
		setDateHour();
	}
	
	//protocol
	public String getProtocol(){
		return protocol;
	}
	
	public void setProtocol(String aProtocol){
		if(aProtocol == null || "".equals(aProtocol) || " ".equals(aProtocol)){
			protocol = "-";
		}else{
			protocol = aProtocol;
		}
	}
	
	//rprotocol
	public String getRprotocol(){
		return rprotocol;
	}
	
	public void setRprotocol(String aRprotocol){
		if(aRprotocol == null || "".equals(aRprotocol) || " ".equals(aRprotocol)){
			rprotocol = "-";
		}else{
			rprotocol = aRprotocol;
		}
	} 
	
	//clientFlag
	public String getClientFlag(){
		return clientFlag;
	}
	public void setClientFlag(String aClientFlag){
		if(aClientFlag.matches("^[0-9]+$")){
			clientFlag = aClientFlag;
		}else{
			clientFlag = "-999";
		}
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
		this.longIp = IpParser1.ip2long(ip);
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
	
	//version
	public long getVersionId(){
		return versionId;
	}
	public void setVersionId(String version){
		versionId = IpParser1.ip2long(version);
	}
	
	//sid
	public String getSid(){
		return sid;
	}
		
	public void setSid(String aSid){
		if(aSid == null || "".equals(aSid) || " ".equals(aSid)){
			sid = "-";
		}else{
			sid = aSid;
		}	
	}
	
	//pvid
	public String getPvid(){
		return pvid;
	}
	
	public void setPvid(String aPvid){
		if(aPvid == null || "".equals(aPvid) || " ".equals(aPvid)){
			pvid = "-";
		}else{ 
			pvid = aPvid;
		}
	}
	
	//config
	public String getConfig(){
		return config;
	}
	
	public void setConfig(String aConfig){
		if(aConfig == null || "".equals(aConfig) || " ".equals(aConfig)){
			config = "-";
		}else{
			config = aConfig;
		}
	}
	
	//url
	public String getUrl(){
		return url;
	}
	
	public void setUrl(String aUrl){
		if(aUrl == null || "".equals(aUrl) || " ".equals(aUrl)){
			url = "-";
		}else{ 
			url = aUrl;
		}
	}
	
	//referUrl
	public String getReferUrl(){
		return referUrl;
	}
	
	public void setReferUrl(String aReferUrl){
		if(aReferUrl == null || "".equals(aReferUrl) || " ".equals(aReferUrl)){
			referUrl = "-";
		}else{
			referUrl = aReferUrl;
		}
	}
	
	//channelId
	public String getChannelId(){
		return channelId;
	}
	
	public void setChannelId(String aChannelId){
		channelId =  channelIdMap.containsKey(aChannelId) ? aChannelId : "1";
	}
	
	//block
	public String getBlock(){
		return block;
	}
	
	public void setBlock(String aBlock){
		if(aBlock == null || "".equals(aBlock) || " ".equals(aBlock)){
			block = "-";
		}else{
			block = aBlock;
		}
	}
	
	//screenWidth
	public String getScreenWidth(){
		return screenWidth;
	}
	
	public void setScreenWidth(String aScreenWidth){
		if(aScreenWidth == null || "".equals(aScreenWidth)){
			screenWidth = "-"; 
		}else{
			screenWidth = aScreenWidth;
		}
	}
	
	//screenHeight
	public String getScreenHeight(){
		return screenHeight;
	}
	
	public void setScreenHeight(String aScreenHeight){
		if(aScreenHeight == null || "".equals(aScreenHeight)){
			screenHeight = "-"; 
		}else{
			screenHeight = aScreenHeight;
		}
	}
	
	//browserWidth
	public String getBrowserWidth(){
		return browserWidth;
	}
	
	public void setBrowserWidth(String aBrowserWidth){
		if(aBrowserWidth == null || "".equals(aBrowserWidth)){
			browserWidth = "-"; 
		}else{
			browserWidth = aBrowserWidth;
		}
	}
	
	//browserHeight
	public String getBrowserHeight(){
		return browserHeight;
	}
	
	public void setBrowserHeight(String aBrowserHeight){
		if(aBrowserHeight == null || "".equals(aBrowserHeight)){
			browserHeight = "-"; 
		}else{
			browserHeight = aBrowserHeight;
		}
	}
	
	//browserPx
	public String getBrowserPx(){
		return browserPx;
	}
	
	public void setBrowserPx(String aBrowserPx){
		if(aBrowserPx == null || "".equals(aBrowserPx)){
			browserPx = "-"; 
		}else{
			browserPx = aBrowserPx;
		}
	}
	
	//browserPy
	public String getBrowserPy(){
		return browserPy;
	}
	
	public void setBrowserPy(String aBrowserPy){
		if(aBrowserPy == null || "".equals(aBrowserPy)){
			browserPy = "-"; 
		}else{
			browserPy = aBrowserPy;
		}
	}
	
	//pagePx
	public String getPagePx(){
		return pagePx;
	}
		
	public void setPagePx(String aPagePx){
		if(aPagePx == null || "".equals(aPagePx)){
			pagePx = "-"; 
		}else{
			pagePx = aPagePx;
		}
	}
	
	//pagePy
	public String getPagePy(){
		return pagePy;
	}
	
	public void setPagePy(String aPagePy){
		if(aPagePy == null || "".equals(aPagePy)){
			pagePy = "-"; 
		}else{
			pagePy = aPagePy;
		}
	}
	
	//ext
	public String getExt(){
		return ext;
	}
		
	public void setExt(String aExt){
		if(aExt == null || "".equals(aExt) || " ".equals(aExt)){
			ext = "-";
		}else{
			ext = aExt;
		}
	}
	
	//userAgent
	public String getUserAgent(){
		return userAgent;
	}
		
	public void setUserAgent(String aUserAgent){
		if(aUserAgent == null || "".equals(aUserAgent) || " ".equals(aUserAgent)){
			userAgent = "-";
		}else{
			userAgent = aUserAgent;
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
	
	//dateId,HourId
	public void setDateHour(){
		String timestampStr = TimeFormat1.toString(timestamp);
		String[] strs = timestampStr.split("\t");
		dateId = strs[1];
		hourId = strs[2];
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
		   .append(hourId).append(sep)
		   .append(provinceId).append(sep)
		   .append(ispId).append(sep)
		   .append(versionId).append(sep)
		   .append(mac).append(sep)
		   .append(protocol).append(sep)
		   .append(rprotocol).append(sep)
		   .append(longIp).append(sep)
		   .append(timestamp).append(sep)
		   .append(fck).append(sep)
		   .append(userId).append(sep)
		   .append(fpc).append(sep)
		   .append(sid).append(sep)
		   .append(pvid).append(sep)
		   .append(config).append(sep)
		   .append(url).append(sep)
		   .append(referUrl).append(sep)
		   .append(channelId).append(sep)
		   .append(block).append(sep)
		   .append(screenWidth).append(sep)
		   .append(screenHeight).append(sep)
		   .append(browserWidth).append(sep)
		   .append(browserHeight).append(sep)
		   .append(browserPx).append(sep)
		   .append(browserPy).append(sep)
		   .append(pagePx).append(sep)
		   .append(pagePy).append(sep)
		   .append(ext).append(sep)
		   .append(userAgent);
		
		return str.toString();
	}
	
	private String protocol = null;
	private String rprotocol = null;
	private String clientFlag = null;
	private long longIp = 0;
	private String fck = null;
	private String mac = null;
	private String userId = null;
	private String fpc = null;
	private long versionId = 0;
	private String sid = null;
	private String pvid = null;
	private String config = null;
	private String url = null;
	private String referUrl = null;
	private String channelId = null;
	private String block = null;
	private String screenWidth = null;
	private String screenHeight = null;
	private String browserWidth = null;
	private String browserHeight = null;
	private String browserPx = null;
	private String browserPy = null;
	private String pagePx = null;
	private String pagePy = null;
	private String ext = null;
	private String userAgent = null;
	private String provinceId = null;
	private String ispId = null;
	private String timestamp = null;
	private String dateId = null;
	private String hourId = null;
	private IpParser1 ipParser = null;
	private HashMap<String,String> channelIdMap = null;
}
