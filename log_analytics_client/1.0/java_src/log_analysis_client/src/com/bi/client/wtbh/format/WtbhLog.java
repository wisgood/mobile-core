package com.bi.client.wtbh.format;

import java.util.regex.Pattern;

import com.bi.client.util.IpParser1;
import com.bi.client.util.MACFormat1;

public class WtbhLog {
	
	public void setFields(String[] fields) throws Exception {
		setSid(fields[WtbhLogEnum.SID.ordinal()]);
		setLongIp(fields[WtbhLogEnum.LONGIP.ordinal()]);
		setTimestamp(fields[WtbhLogEnum.TIMESTAMP.ordinal()]);
		setPgid(fields[WtbhLogEnum.PGID.ordinal()]);
		setPvs(fields[WtbhLogEnum.PVS.ordinal()]);
		setMac(fields[WtbhLogEnum.MAC.ordinal()]);
		setIh(fields[WtbhLogEnum.IH.ordinal()]);
		setIm(fields[WtbhLogEnum.IM.ordinal()]);
		setPbpr(fields[WtbhLogEnum.PBPR.ordinal()]);
		setPfpr(fields[WtbhLogEnum.PFPR.ordinal()]);
		setPlayTime(fields[WtbhLogEnum.PLAY_TIME.ordinal()]);
		setPstm(fields[WtbhLogEnum.PSTM.ordinal()]);
		setPtp(fields[WtbhLogEnum.PTP.ordinal()]);
		setVersionId(fields[WtbhLogEnum.VERSION.ordinal()]);
		setProvinceId();
		setIspId();
	}
	
	//sid
	public String getSid(){
		return sid;
	}
	
	public void setSid(String sid){
		if(!Pattern.matches("^\\d+$", sid) || "".equals(sid) || sid == null){
			sid = "-";
		}else{
			this.sid = sid;
		}	
	}
	
	//longIp
	public long getLongIp(){
		return longIp;
	}
	
	public void setLongIp(String longIp){
		this.longIp = Long.parseLong(longIp);
	}
	
	//provinceId
	public String  getProvinceId(){
		return provinceId;
	}
	
	public void setProvinceId(){
		provinceId = ipParser.getAreaId(longIp);
	}
	
	//ispId
	public String getIspId(){
		return ispId;
	}
	
	public void setIspId(){
		ispId = ipParser.getIspId(longIp);
	}
	
	//timestamp
	public String getTimestamp(){
		return timestamp;
	}
	
	public void setTimestamp(String timestamp){
		this.timestamp = timestamp;
	}
	
	//pgid
	public String getPgid(){
		return pgid;
	}
	
	public void setPgid(String pgid){
		if(pgid == null || "".equals(pgid)){
			this.pgid = "-";
		}else{
			this.pgid = pgid;
		}
	}
	
	//pvs
	public String getPvs(){
		return pvs;
	}
	
	public void setPvs(String pvs){
		if(pvs == null || "".equals(pvs)){
			this.pvs = "-";
		}else{
			this.pvs = pvs;
		}
	}
	
	//mac
	public String getMac(){
		return mac;
	}
	
	public void setMac(String amac){
		mac = MACFormat1.macFormat(amac);
	}
	
	//ih
	public String getIh(){
		return ih;
	}
	
	public void setIh(String ih){
		if(ih == null || "".equals(ih)){
			this.ih = "-";
		}else{
			this.ih = ih;
		}
	}
	
	public String getIm(){
		return im;
	}
	
	public void setIm(String im){
		if(im == null || "".equals(im)){
			this.im = "-";
		}else{
			this.im = im;
		}
	}
	
	//pbpr
	public String getPbpr(){
		return pbpr;
	}
	
	public void setPbpr(String pbpr){
		if(pbpr == null || "".equals(pbpr)){
			this.pbpr = "-";
		}else{
			this.pbpr = pbpr;
		}
	}
	
	//pfpr
	public String getPfpr(){
		return pfpr;
	}
	
	public void setPfpr(String pfpr){
		if(pfpr == null || "".equals(pfpr)){
			this.pfpr = "-";
		}else{
			this.pfpr = pfpr;
		}
	}
	
	//play time
	public String getPlayTime(){
		return playTime;
	}
	
	public void setPlayTime(String playTime){
		if(Pattern.matches("^\\d+$", playTime)){
			this.playTime = playTime;
		}else{
			this.playTime = "0";
		}
	}
	
	//pstm
	public String getPstm(){
		return pstm;
	}
	
	public void setPstm(String pstm){
		if(Pattern.matches("^\\d+$", pstm)){
			this.pstm = pstm;
		}else{
			this.pstm = "0";
		}
	}
	
	//ptp
	public String getPtp(){
		return ptp;
	}
	
	public void setPtp(String ptp){
		if(ptp == null || "".equals(ptp)){
			this.ptp = "-";
		}else{
			this.ptp = ptp;
		}
	}
	
	//version
	public long getVersionId(){
		return versionId;
	}
	public void setVersionId(String version){
		versionId = IpParser1.ip2long(version);
	}
	
	
	public String toString(){
		StringBuilder str = new StringBuilder();
		String sep = "\t";
		str.append(versionId).append(sep)
		   .append(provinceId).append(sep)
		   .append(mac).append(sep)
		   .append(ispId).append(sep)
		   .append(playTime).append(sep)
		   .append(sid).append(sep)
		   .append(longIp).append(sep)
		   .append(timestamp).append(sep)
		   .append(pgid).append(sep)
		   .append(pvs).append(sep)
		   .append(ih).append(sep)
		   .append(im).append(sep)
		   .append(pbpr).append(sep)
		   .append(pfpr).append(sep)
		   .append(pstm).append(sep)
		   .append(ptp);
		
		return str.toString();
	}
	
	
	//set IpParser
	public void setIpParser(IpParser1 aIpParser){
		ipParser = aIpParser;
	}
	
	private String sid = null;
	private long longIp = 0;
	private String timestamp = null;
	private String pgid = null;
	private String pvs = null;
	private String mac = null;
	private String ih = null;
	private String im = null;
	private String pbpr = null;
	private String pfpr = null;
	private String playTime = null;
	private String pstm = null;
	private String ptp = null;
	private long versionId = 0l;
	private String provinceId = null;
	private String ispId = null;
	private IpParser1 ipParser = null;
	
}
