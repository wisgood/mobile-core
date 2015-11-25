package com.bi.ad.logs.click.format;

import java.util.HashMap;
import com.bi.ad.comm.util.format.*;


public class ClickLog {

	private static HashMap<String, Integer> versionMap = new HashMap<String, Integer>();
	
	static {
		versionMap.put("130000", new Integer(12));
		versionMap.put("130001", new Integer(13));
		versionMap.put("130002", new Integer(14));
		versionMap.put("130003", new Integer(14));
		versionMap.put("130004", new Integer(15));
		versionMap.put("130005", new Integer(18));
		versionMap.put("130006", new Integer(23));
	}
	
	public static boolean isClickLog (String logType){
		 if (versionMap.containsKey(logType))
			 return true;
		 return false;	 
	}
	
	public static int getLogLength(String logType){
		return versionMap.get(logType);
	}
	
	public static void checkLengthEligible (String logType, int size) throws Exception {
		if(isClickLog(logType)){
			if(versionMap.get(logType).intValue() != size)
				throw new Exception("Fields num not match");
		}
	}
		
	public ClickLog() {		
	}
	
	
	public void setFields (String[] fields) throws Exception {
		setLogType(fields[0]);
		setTime(fields[1]);		
		setIp(fields[2]);
		setMac(fields[3]);
		setFck(fields[4]);
		setAdp(fields[5]);	
		setUid(fields[6]);
		setVer(fields[7]);
		setAd(fields[9]);
		setMid(fields[10]);
		setCid(fields[11]);
		setCityId();
		setCopyright();
		setMediaType();
		if (logType.equals("130006")){                      //version130005只添加了一些hermes自己调试用的字段，所以不做处理
			setOS(fields[ClickLogEnum6.OS.ordinal()]);
			setOsVer(fields[ClickLogEnum6.OS_VER.ordinal()]);
			setImei(fields[ClickLogEnum6.IMEI.ordinal()]);
			setFudid(fields[ClickLogEnum6.FUDID.ordinal()]);
			setIdfa(fields[ClickLogEnum6.IDFA.ordinal()]);
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
	public String getAdp() {
		return adp;
	}

	public void setAdp(String adp) throws Exception{
		if ( !adpInfo.getAdpInfo().containsKey(adp))
			throw new Exception("adp missing");
		this.adp =  adp;
	}

	public String getAd() {
		return ad;
	}

	public void setAd(String ad) throws Exception {
		if(! adInfo.getAdInfo().contains(ad))
			throw new Exception("ad missing");
		this.ad = ad;
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
	    if(logType.equals("130002")){
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

	//media type
	public String getMediaType() {
		return mediaType;
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
			
	// set an MediaInfo instance
	public void setMediaInfo(MediaInfo mediaInfo) {
		this.mediaInfo = mediaInfo;
	}
	
	// set an LiveChannel instance
	public void setLiveInfo(LiveChannelInfo liveInfo){
		this.liveInfo = liveInfo;	
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
	
	//fudid
	public void setFudid(String aFudid){
		if(!"".equals(aFudid) && aFudid != null){
			fudid = aFudid;
		}
	}
	
	public String getFudid(){
		return fudid;
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
	
	// output format
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
		   .append(adp).append(sep)
		   .append(ad).append(sep)
		   .append(os).append(sep)
		   .append(osVer).append(sep)
		   .append(imei).append(sep)
		   .append(fudid).append(sep)
		   .append(idfa);
		   
		return str.toString();
	}
	
    public void setCopyright() {
        if(mediaInfo.getMediaInfo().containsKey(mid)){
            this.copyright = mediaInfo.getMediaInfo().get(mid).getCopyright();
        }else{
            this.copyright = "1";  //with copyright
        }
    }

    
    private void setMediaType() {
        // "80" represent live channel
        // "81" represent optimize channel
        // "82" represent unknown
        if( (! cid.equals("-") && logType.equals("130002")) || cid.equals("1")){
            this.mediaType = "80";
        }else{
            if(mid.equals("-")){
                if(adpInfo.getAdpInfo().get(adp).getOptFlag().equals("1")){ // "1" means optimized code
                    this.mediaType = "81";
                }else{
                    this.mediaType = "82";
                }
            }else{
                this.mediaType =  mediaInfo.getMediaInfo().get(mid).getType();
            }       
        }
    }
	private String  logType = null;
	private String  time    = null;
	private String  ip	    = null;
	private String  cityId  = null;
	private String  mac	    = null;
	private String  fck     = null;
	private String  adp	    = null;
	private String  uid	    = null;
	private String  ver     = null;
	private String  ad		= null;
	private String  mid	    = null;
	private String  cid	    = null;
	private IpArea  ipArea  = null;
    private AdpInfo adpInfo = null;
    private AdInfo  adInfo  = null;
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
