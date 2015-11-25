package com.bi.ad.logs.deliver.format;

import java.util.HashMap;

import com.bi.ad.comm.util.format.AdInfo;
import com.bi.ad.comm.util.format.AdpInfo;
import com.bi.ad.comm.util.format.IpArea;
import com.bi.ad.comm.util.format.LiveChannelInfo;
import com.bi.ad.comm.util.format.MACFormat;
import com.bi.ad.comm.util.format.MatInfo;
import com.bi.ad.comm.util.format.MediaInfo;
import com.bi.ad.comm.util.format.TimeFormat;


public class DeliverLog {

	private static HashMap<String, Integer> versionMap = new HashMap<String, Integer>();
	
	static {
		versionMap.put("110000", new Integer(11));
		versionMap.put("110001", new Integer(12));
		versionMap.put("110002", new Integer(13));
		versionMap.put("110003", new Integer(13));
		versionMap.put("110004", new Integer(14));
		versionMap.put("110005", new Integer(17));
		versionMap.put("110006", new Integer(22));
	}
	
	public static boolean isDeliverLog (String logType){
		 if (versionMap.containsKey(logType))
			 return true;
		 return false;	 
	}
	
	public static void checkLengthEligible (String logType, int size) throws Exception {
		if(isDeliverLog(logType)){
			if(versionMap.get(logType).intValue() != size)
				throw new Exception("Fields num not match");
		}
	}
		
	public DeliverLog() {		
	}
	
	
	public void setFields (String[] fields) throws Exception {
		setLogType(fields[0]);
		setTime(fields[1]);		
		setIp(fields[2]);
		setMac(fields[3]);
//		setFck(fields[4]);
		setAdpm(fields[5]);
		setUid(fields[6]);
		setVer(fields[7]);
		setMid(fields[9]);
		setCid(fields[10]);
		setCityId();
		setCopyright();
		setMediaType();
		if (logType.equals("110006") && mobileFlag){          //if it's mobile ad position, then use the fudid as the fck
			setFck(fields[DeliverLogEnum6.FUDID.ordinal()]);  //else use the fck
		}else{
			setFck(fields[4]);
		}
		if (logType.equals("110006")){                      //version130005只添加了一些hermes自己调试用的字段，所以不做处理
			setOS(fields[DeliverLogEnum6.OS.ordinal()]);
			setOsVer(fields[DeliverLogEnum6.OS_VER.ordinal()]);
			setImei(fields[DeliverLogEnum6.IMEI.ordinal()]);
			setIdfa(fields[DeliverLogEnum6.IDFA.ordinal()]);
		}
	}
		

	//logtype
	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}
	
	//time
	public String getTime() {
		return time;
	}

	public void setTime(String timeStamp) {		
		this.time = TimeFormat.toString(timeStamp);
	}	
	
	//ip 
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = Long.toString(IpArea.ip2long(ip));
	}
	
	// city
	public String getCityId(){
		return cityId;
	}
	
	public void setCityId(){
		cityId =  ipArea.getAreaID(Long.parseLong(ip));
	}
		
	//mac
	public String getMac() {
		return mac;
	}

	public void setMac(String aMac) {
		mac = MACFormat.macFormat(aMac);
	}
		
	//fck
	public String getFck() {
		return fck;
	}

	public void setFck(String fck) {
	    this.fck =  MACFormat.isCorrectFck(fck) ? fck : "-";
	}

	//ad_ap_material
	public String getAdpm() {
		return adpm;
	}

	public void setAdpm(String adpm) throws Exception {
		this.adpm =  filterRawAdpm(adpm);
	}

	//uid
	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid == null ? "-" : uid;
	}
	
	//version
	public String getVer() {
		return ver;
	}

	public void setVer(String ver) {
		this.ver = ver == null ? "-" : ver;
	}
	
	//mid
	public String getMid() {
		return mid;
	}

	public void setMid(String mid){
		this.mid = mediaInfo.getMediaInfo().containsKey(mid) ? mid : "-";
	}
	
	//cid
	public String getCid() {
		return cid;
	}

    public void setCid(String cid) {
        if(logType.equals("110002")){
            this.cid = liveInfo.getLiveInfo().contains(cid)? cid : "-";
        }else{
            this.cid = cid.equals("0") || cid.equals("1") || cid.equals("2") || cid.equals("3") ?
                            cid : "-";
        }
    }
	
	//copyright
	public String getCopyright() {
		return copyright;
	}

	public void setCopyright() {
		if(mediaInfo.getMediaInfo().containsKey(mid)){
		    this.copyright = mediaInfo.getMediaInfo().get(mid).getCopyright();
		}else{
		    this.copyright = "1";  //with copyright
		}
	}

	//media type
	public String getMediaType() {
		return mediaType;
	}

	public void setMediaType() {
		// "80" represent live channel
		// "-" represent optimize channel && 'unknown'
		if( (! cid.equals("-") && logType.equals("110002")) || cid.equals("1")){
			this.mediaType = "80";
		}else{
		    this.mediaType = mid.equals("-")? "-": mediaInfo.getMediaInfo().get(mid).getType();
		}
	}

	//set OS
	public void setOS(String os){
		if(!"".equals(os) && os != null ){
			this.os = os;
		}
	}
		
	public String getOS(){
		return os;
	}

	//osVer
	public void setOsVer(String aOsVer){
		if(!"".equals(aOsVer) && aOsVer != null){
			osVer = aOsVer;
		}
	}
		
	public String getOsVer(){
		return osVer;
	}
		
	//imei
	public void setImei(String aImei){
		if(!"".equals(aImei) && aImei != null){
			imei = aImei;
		}
	}
		
	public String getImei(){
		return imei;
	}
		
	//idfa
	public void setIdfa(String aIdfa){
		if(!"".equals(aIdfa) && aIdfa != null){
			idfa = aIdfa;
		}
	}
		
	public String getIdfa(){
		return idfa;
	}
	
	public String toString(){
		StringBuilder str = new StringBuilder();
		String 		  sep = "\t";
		str.append(logType).append(sep)
		   .append(time).append(sep)      // timestamp date_id hour_id
		   .append(cityId).append(sep)
		   .append(mediaType).append(sep)
		   .append(mid).append(sep)
		   .append(copyright).append(sep)
		   .append(cid).append(sep)	
		   .append(ip).append(sep)
		   .append(mac).append(sep)
		   .append(fck).append(sep)
		   .append(uid).append(sep)
		   .append(ver).append(sep)
		   .append(adpm).append(sep)
		   .append(os).append(sep)
		   .append(osVer).append(sep)
		   .append(imei).append(sep)
		   .append(fudid).append(sep)
		   .append(idfa);
		return str.toString();
	}
	
	// set an IpArea instance
	public void setIpArea(IpArea aIpArea) {
		ipArea = aIpArea;
	}
	
	// set an AdpInfo instance
	public void setAdpInfo(AdpInfo adpInfo) {
		this.adpInfo = adpInfo;
	}

	// set an AdInfo instance
	public void setAdInfo(AdInfo adInfo) {
		this.adInfo = adInfo;
	}
	
	
	// set an matInfo instance
	public void setMatInfo(MatInfo matInfo) {
		this.matInfo = matInfo;
	}
	
	// set an MediaInfo instance
	public void setMediaInfo(MediaInfo mediaInfo) {
		this.mediaInfo = mediaInfo;
	}
	
	// set an LiveChannel instance
	public void setLiveInfo(LiveChannelInfo liveInfo){
		this.liveInfo = liveInfo;	
	}
	
	//setMobileFlag
	public void setMobileFlag(String ap){
		if (ap.contains("ape") || ap.contains("ipe") || ap.contains("ipd")){
			mobileFlag = true;
		}
	}

	private String filterRawAdpm (String admp) throws Exception{
		String 	ad  = null;
		String  ap  = null;
		String  mat = null;				
		StringBuilder  resultStr = new StringBuilder(); 
		String[] arrApAdMat = admp.split("\\|");
		
		for (int i = 0; i < arrApAdMat.length; i++) {
			String[] apAdMat = arrApAdMat[i].split("[:;]");
			ap = apAdMat[0];
			if (! adpInfo.getAdpInfo().containsKey(ap))
				continue;
			setMobileFlag(ap);
			if (resultStr.length() > 0 )
				resultStr.append("|");
			resultStr.append(ap).append(":");
			for (int j = 1; j < apAdMat.length; j++) {
				String[] adMat = apAdMat[j].split("#");
				ad  = adMat[0];
				mat = adMat[1]; 
				if (! adInfo.getAdInfo().contains(ad))
					ad = "-";	
				if( ! matInfo.getMatInfo().containsKey(mat))
					mat = "-";
				resultStr.append(ad).append("#").append(mat);
				if( j < (apAdMat.length -1 ))
					resultStr.append(";");
			}
		}
		
		if (resultStr.length() != 0)
			return resultStr.toString();
		else
			throw new Exception("adp code error");
	}
	
	private boolean mobileFlag = false;
	private String  logType = null;
	private String  time    = null;
	private String  ip	    = null;
	private String  cityId  = null;
	private String  mac	    = null;
	private String  fck     = null;
	private String  adpm    = null;
	private String  uid	    = null;
	private String  ver     = null;
	private String  mid	    = null;
	private String  cid	    = null;
	private IpArea  ipArea  = null;
    private AdpInfo adpInfo = null;
    private AdInfo  adInfo  = null;
    private MatInfo matInfo = null;
	private String  copyright = null;
	private String  mediaType = null;
    private MediaInfo mediaInfo = null;
    private LiveChannelInfo liveInfo = null;
    private String os = "-";
    private String osVer = "-";
    private String imei = "-";
    private String fudid = "-";
    private String idfa = "-";
}
