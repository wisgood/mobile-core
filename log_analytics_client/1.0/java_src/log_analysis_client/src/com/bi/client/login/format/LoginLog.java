package com.bi.client.login.format;

import static com.bi.client.util.IpParser.version2long;
import static com.bi.client.util.MACFormat.macFormat;
import static com.bi.client.util.IpParser.ip2long;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.bi.client.util.*;

public class LoginLog {
    
    LoginLog(int length) throws Exception{
        if(length != fieldsLength)
            throw new Exception("Fields not match");
    }
   
    public void setIpParser (IpParser ipParser){
        this.ipParser = ipParser;
    }
    
    public void setStatDate(String statDate) throws ParseException {
        date = statDate;
        minLoginTime = (int)(new SimpleDateFormat("yyyyMMdd").parse(statDate).getTime() / 1000);        
        maxLogoutTime = minLoginTime + 86400;       
    } 
    
    public void setFields(String[] fields) throws Exception {
       setUserName(fields[0]);
       setUserType(fields[1]);
       setVersion(fields[3]);
       setSourceIp(fields[4]);
       setArea();
       setIsp();
       setLogoutReason(fields[5]);
       setNetworkType(fields[6]);
       setHsIp(fields[7]);
       setLoginTime(fields[8]);
       setLogoutTime(fields[9]);
       setChannelId(fields[10]);
       setMac(fields[11]);
       setClientVersion(fields[12]);
       setUid(fields[13]);
       setUploadFlux(fields[15]);
       setDownloadFlux(fields[16]);
    }
    
    public String toString(){
       StringBuilder resultStr = new StringBuilder();
       resultStr.append(date + sep)
                .append(clientVersion + sep)  
                .append(area + sep)
                .append(mac + sep)
                .append(isp + sep)                
                .append(userName + sep)
                .append(userType + sep)
                .append("-" + sep)
                .append(version  + sep)
                .append(sourceIp + sep)
                .append(logoutReason + sep)
                .append(networkType + sep)
                .append(hsIp + sep)
                .append(loginTime + sep)
                .append(logoutTime + sep)
                .append(channelId + sep)
                .append(uid + sep)
                .append("-" + sep)
                .append(uploadFlux + sep)
                .append(downloadFlux);
       return resultStr.toString();
    }
   
    private void setUserName(String userName) {
        this.userName = userName == null ? "-" : userName ;
    }
 
    private void setUserType(String userType) {
        this.userType = userType.equals("1") || userType.equals("2") ?
                        userType : "2";
    }

    private void setVersion(String version) {
        this.version = version == null ? "-" : version;
    }

    private void setSourceIp(String aSourceIp) {
        this.sourceIp = ip2long(aSourceIp);
    }

    private void setLogoutReason(String logoutReason) {
        this.logoutReason = logoutReason == null ? "-" : logoutReason;
    }

    private void setNetworkType(String networkType) {
        this.networkType = networkType == null ?  "-" : networkType;
    }

    private void setHsIp(String aHsIp) {
        this.hsIp = ip2long(aHsIp);
    }

    private void setLoginTime(String aLoginTime) {
        long unixSecond = Long.parseLong(aLoginTime);
        this.loginTime = unixSecond >= minLoginTime && unixSecond <= maxLogoutTime ?
                         unixSecond : minLoginTime;
    }

    private void setLogoutTime(String aLogoutTime) {
        long unixSecond  = Long.parseLong(aLogoutTime);
        this.logoutTime = unixSecond >= minLoginTime && unixSecond <= maxLogoutTime?
                          unixSecond : maxLogoutTime;
    }

    private void setChannelId(String channelId) {
        this.channelId = (channelId == null || channelId.equals("0"))? "1" : channelId;
 
    }

    private void setMac(String mac) {
        this.mac = macFormat(mac.substring(4));
    }

    private void setClientVersion(String aClientVersion) {
        this.clientVersion = version2long(aClientVersion);
    }

    private void setUid(String uid) {
        this.uid = uid == null ? "-" : uid;
    }

    private void setUploadFlux(String uploadFlux) {
        this.uploadFlux = Integer.parseInt(uploadFlux) >= 0 ? uploadFlux : "0";
    }

    private void setDownloadFlux(String downloadFlux) {
        this.downloadFlux = Integer.parseInt(downloadFlux) >= 0 ? downloadFlux: "0";
    }

    private void setArea() {
        this.area = ipParser.getAreaId(sourceIp);
    }

    private void setIsp() {
        this.isp = ipParser.getIspId(sourceIp);
    }
    private final static int fieldsLength = 17; 
    private final static String sep = "\t";
    private String userName = null;
    private String userType = null;
    private String version  = null;
    private long   sourceIp = 0;
    private String logoutReason = null;
    private String networkType  = null;
    private long   hsIp         = 0;
    private long loginTime      = 0;
    private long logoutTime     = 0;
    private String channelId    = null;
    private String mac          = null;
    private long clientVersion  = 0;
    private String uid          = null;
    private String uploadFlux   = null;
    private String downloadFlux = null;
    private String area         = null;
    private String isp          = null;
    private IpParser ipParser   = null;
    private String date         = "";
    private int    minLoginTime = 0;
    private int   maxLogoutTime = 0;        
}
