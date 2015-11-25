package com.bi.client.taskstat.format;

import java.util.regex.Pattern;

import com.bi.client.util.IpParser1;
import com.bi.client.util.MACFormat1;

public class TaskstatLog {
	
	public void setFields(String[] fields) throws Exception {
		setSid(fields[TaskstatLogEnum.SID.ordinal()]);
		setLongIp(fields[TaskstatLogEnum.LONGIP.ordinal()]);
		setTimestamp(fields[TaskstatLogEnum.TIMESTAMP.ordinal()]);
		setPgid(fields[TaskstatLogEnum.PGID.ordinal()]);
		setPvs(fields[TaskstatLogEnum.PVS.ordinal()]);
		setBtd(fields[TaskstatLogEnum.BTD.ordinal()]);
		setBtp(fields[TaskstatLogEnum.BTP.ordinal()]);
		setBtsd(fields[TaskstatLogEnum.BTSD.ordinal()]);
		setBtst(fields[TaskstatLogEnum.BTST.ordinal()]);
		setDr(fields[TaskstatLogEnum.DR.ordinal()]);
		setFtd(fields[TaskstatLogEnum.FTD.ordinal()]);
		setFtp(fields[TaskstatLogEnum.FTP.ordinal()]);
		setFtsd(fields[TaskstatLogEnum.FTSD.ordinal()]);
		setFtst(fields[TaskstatLogEnum.FTST.ordinal()]);
		setMac(fields[TaskstatLogEnum.MAC.ordinal()]);
		setMdt(fields[TaskstatLogEnum.MDT.ordinal()]);
		setUr(fields[TaskstatLogEnum.UR.ordinal()]);
		setVersionId(fields[TaskstatLogEnum.VERSION.ordinal()]);
		setProvinceId();
		setIspId();
	}
	
	//sid
	public String getSid(){
		return sid;
	}
	
	public void setSid(String sid){
		if(!Pattern.matches("^\\d+$", sid) || "".equals(sid) || sid == null){
			this.sid = "-";
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
	
	//btd
	public String getBtd(){
		return btd;
	}
	
	public void setBtd(String btd){
		if(btd == null || "".equals(btd)){
			this.btd = "-";
		}else{
			this.btd = btd;
		}
	}
	
	//btp
	public String getBtp(){
		return btp;
	}
	
	public void setBtp(String btp){
		if(btp == null || "".equals(btp)){
			this.btp = "-";
		}else{
			this.btp = btp;
		}
	}
	
	//btsd
	public String getBtsd(){
		return btsd;
	}
	
	public void setBtsd(String btsd){
		if(btsd == null || "".equals(btsd)){
			this.btsd = "-";
		}else{
			this.btsd = btsd;
		}
	}
	
	//btst
	public String getBtst(){
		return btst;
	}
	
	public void setBtst(String btst){
		if(btst == null || "".equals(btst)){
			this.btst = "-";
		}else{
			this.btst = btst;
		}
	}
	
	//Dr
	public String getDr(){
		return dr;
	}
	
	public void setDr(String dr){
		if(dr == null || "".equals(dr)){
			this.dr = "-";
		}else{
			this.dr = dr;
		}
	}
	
	//ftd
	public String getFtd(){
		return ftd;
	}
	
	public void setFtd(String ftd){
		if(ftd == null || "".equals(ftd)){
			this.ftd = "-";
		}else{
			this.ftd = ftd;
		}
	}
	
	//ftp
	public String getFtp(){
		return ftp;
	}
	
	public void setFtp(String ftp){
		if(ftp == null || "".equals(ftp)){
			this.ftp = "-";
		}else{
			this.ftp = ftp;
		}
	}
	
	//ftsd
	public String getFtsd(){
		return ftsd;
	}
	
	public void setFtsd(String ftsd){
		if(ftsd == null || "".equals(ftsd)){
			this.ftsd = "-";
		}else{
			this.ftsd = ftsd;
		}
	}
	
	//ftst
	public String getFtst(){
		return ftst;
	}
	
	public void setFtst(String ftst){
		if(ftst == null || "".equals(ftst)){
			this.ftst = "-";
		}else{
			this.ftst = ftst;
		}
	}
	
	//mdt
	public String getMdt(){
		return mdt;
	}
	
	public void setMdt(String mdt){
		if(mdt == null || "".equals(mdt)){
			this.mdt = "-";
		}else{
			this.mdt = mdt;
		}
	}
	
	//ur
	public String getUr(){
		return ur;
	}
	
	public void setUr(String ur){
		if(ur == null || "".equals(ur)){
			this.ur = "-";
		}else{
			this.ur = ur;
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
		   .append(sid).append(sep)
		   .append(longIp).append(sep)
		   .append(timestamp).append(sep)
		   .append(pgid).append(sep)
		   .append(pvs).append(sep)
		   .append(btd).append(sep)
		   .append(btp).append(sep)
		   .append(btsd).append(sep)
		   .append(btst).append(sep)
		   .append(dr).append(sep)
		   .append(ftd).append(sep)
		   .append(ftp).append(sep)
		   .append(ftsd).append(sep)
		   .append(ftst).append(sep)
		   .append(mdt).append(sep)
		   .append(ur);
		
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
	private String btd = null;
	private String btp = null;
	private String btsd = null;
	private String btst = null;
	private String dr = null;
	private String ftd = null;
	private String ftp = null;
	private String ftsd = null;
	private String ftst = null;
	private String mdt = null;
	private String ur = null;
	private long versionId = 0l;
	private String provinceId = null;
	private String ispId = null;
	private IpParser1 ipParser = null;
}
