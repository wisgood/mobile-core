/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: Pv2Log.java 
* @Package com.bi.client.pv2.format 
* @Description: 对日志名进行处理
* @author limm
* @date 2013-9-10 下午2:05:00 
* @input:输入日志路径/2013-9-10
* @output:输出日志路径/2013-9-10
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.client.pv2.format;

import java.util.HashMap;
import java.util.regex.Pattern;

import com.bi.client.util.IpParser1;
import com.bi.client.util.MACFormat1;
import com.bi.client.util.TimeFormat1;

/** 
 * @ClassName: Pv2Log 
 * @Description: There are some methods to format the pv2Log 
 * @author limm 
 * @date 2013-9-10 下午2:05:00  
 */
public class Pv2Log {
	
	public void setFields(String[] fields){
		setProtocol(fields[Pv2LogEnum.PROTOCOL.ordinal()]);
		setRprotocol(fields[Pv2LogEnum.RPROTOCOL.ordinal()]);
		setTimestamp(fields[Pv2LogEnum.TIMESTAMP.ordinal()]);
		setLongIp(fields[Pv2LogEnum.IP.ordinal()]);
		setFck(fields[Pv2LogEnum.FCK.ordinal()]);
		setMac(fields[Pv2LogEnum.MAC.ordinal()]);
		setUserId(fields[Pv2LogEnum.USER_ID.ordinal()]);
		setFpc(fields[Pv2LogEnum.FPC.ordinal()]);
		setVersionId(fields[Pv2LogEnum.VERSION.ordinal()]);
		setSid(fields[Pv2LogEnum.SID.ordinal()]);
		setPvid(fields[Pv2LogEnum.PVID.ordinal()]);
		setConfig(fields[Pv2LogEnum.CONFIG.ordinal()]);
		setUrl(fields[Pv2LogEnum.URL.ordinal()]);
		setReferUrl(fields[Pv2LogEnum.REFERURL.ordinal()]);
		setChannelId(fields[Pv2LogEnum.CHANNEL_ID.ordinal()]);
		setVtime(fields[Pv2LogEnum.VTIME.ordinal()]);
		setExt(fields[Pv2LogEnum.EXT.ordinal()]);
		setUserAgent(fields[Pv2LogEnum.USERAGENT.ordinal()]);
		setStep(fields[Pv2LogEnum.STEP.ordinal()]);
		setSestep(fields[Pv2LogEnum.SESTEP.ordinal()]);
		setSeidCount(fields[Pv2LogEnum.SEIDCOUNT.ordinal()]);
		setTa(fields[Pv2LogEnum.TA.ordinal()]);
		setProvinceId();
		setIspId();
		setDateHour();
	}
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
	
	public String getTimestamp(){
		return timestamp;
	}
	public void setTimestamp(String timestamp){
		this.timestamp = timestamp;
	}
	
	public long getLongIp(){
		return longIp;
	}
	public void setLongIp(String ip){
		this.longIp = IpParser1.ip2long(ip);
	}

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
	
	public String getMac(){
		return mac;
	}
	public void setMac(String amac){
		mac = MACFormat1.macFormat(amac);
	}
	
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
	
	public long getVersionId(){
		return versionId;
	}
	public void setVersionId(String version){
		versionId = IpParser1.ip2long(version);
	}
	
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
	
	public String getChannelId(){
		return channelId;
	}
	public void setChannelId(String aChannelId){
		channelId =  channelIdMap.containsKey(aChannelId) ? aChannelId : "1";
	}
	
	public String getVtime(){
		return vtime;
	}
	public void setVtime(String aVtime){
		if(aVtime == null || "".equals(aVtime) || " ".equals(aVtime)){
			vtime = "-";
		}else{
			vtime = aVtime;
		}
	}
	
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
	
	public String getStep(){
		return step;
	}
	public void setStep(String aStep){
		if(aStep == null || !Pattern.matches("^\\d+", aStep)){
			step = "0";
		}else{
			step = aStep;
		}
	}
	
	public String getSestep(){
		return sestep;
	}
	public void setSestep(String aSestep){
		if(aSestep == null || !Pattern.matches("^\\d+", aSestep)){
			sestep = "0";
		}else{
			sestep = aSestep;
		}
	}
	
	public String getSeidCount(){
		return seidCount;
	}
	public void setSeidCount(String aSeidCount){
		if(aSeidCount == null || !Pattern.matches("^\\d+", aSeidCount)){
			seidCount = "0";
		}else{
			seidCount = aSeidCount;
		}
	}
	
	public String getTa(){
		return ta;
	}
	public void setTa(String aTa){
		if(aTa == null || "".equals(aTa) || " ".equals(aTa)){
			ta = "-";
		}else{
			ta = aTa;
		}
	}
	
	public String  getProvinceId(){
		return provinceId;
	}
	public void setProvinceId(){
		provinceId = ipParser.getAreaId(longIp);
	}
	
	public String  getIspId(){
		return ispId;
	}
	public void setIspId(){
		ispId = ipParser.getIspId(longIp);
	}
	
	public void setDateHour(){
		String timestampStr = TimeFormat1.toString(timestamp);
		String[] strs = timestampStr.split("\t");
		dateId = strs[1];
		hourId = strs[2];
	}
	
	public void setIpParser(IpParser1 aIpParser){
		ipParser = aIpParser;
	}
	
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
		   .append(timestamp).append(sep)
		   .append(longIp).append(sep)
		   .append(fck).append(sep)
		   .append(userId).append(sep)
		   .append(fpc).append(sep)
		   .append(sid).append(sep)
		   .append(pvid).append(sep)
		   .append(config).append(sep)
		   .append(url).append(sep)
		   .append(referUrl).append(sep)
		   .append(channelId).append(sep)
		   .append(vtime).append(sep)
		   .append(ext).append(sep)
		   .append(userAgent).append(sep)
		   .append(step).append(sep)
		   .append(sestep).append(sep)
		   .append(seidCount).append(sep)
		   .append(ta);
		
		return str.toString();
	}
	
	private String protocol = null;
	private String rprotocol = null;
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
	private String ext = null;
	private String userAgent = null;
	private String vtime = null;
	private String step = null;
	private String sestep = null;
	private String seidCount = null;
	private String ta = null;
	private String provinceId = null;
	private String ispId = null;
	private String timestamp = null;
	private String dateId = null;
	private String hourId = null;
	private IpParser1 ipParser = null;
	private HashMap<String, String> channelIdMap = null;
}
