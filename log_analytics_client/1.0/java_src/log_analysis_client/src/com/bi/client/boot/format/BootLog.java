package com.bi.client.boot.format;

import static com.bi.client.util.IpParser.ip2long;
import static com.bi.client.util.IpParser.version2long;
import static com.bi.client.util.MACFormat.macFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.bi.client.util.*;

public class BootLog {
    
    BootLog(int length) throws Exception{
        this.length = length;
        if(!(length == 9 || length == 11 || length == 12))
            throw new Exception("Fields not match");
    }
   
    public void setIpParser (IpParser ipParser){
        this.ipParser = ipParser;
    }
    
    public void setStatDate(String stateDate) throws ParseException {
        time = (int)(new SimpleDateFormat("yyyyMMdd").parse(stateDate).getTime() / 1000);
        date = stateDate;
    } 
    
    public void setFileds(String[] fields){
       setTime(fields[0]);
       setIp(fields[1]);
       setMac(fields[2]);
       setArea();
       setIsp();
       setVersion(fields[3]);
       setChannel(fields[4]);
       setOS(fields[5]);
       setStartType(fields[6]);
       setErrorCode(fields[7]);
       setHc(fields[8]);
       if(length > 9)
           setAccelerate(fields[9]);
       if(length >10)
           setYy(fields[10]);
       if(length > 11)
           setTrayLimit(fields[11]); 
    }
    
    public String toString(){
        StringBuilder resultStr = new StringBuilder();
        resultStr.append(date + sep)
                 .append(version + sep)
                 .append(area + sep)
                 .append(mac + sep)
                 .append(isp + sep)
                 .append(startType + sep)
                 .append(time + sep)
                 .append(ip + sep)
                 .append(channel + sep)
                 .append(OS + sep)
                 .append(errorCode + sep)
                 .append(hc + sep)
                 .append(accelerate + sep)
                 .append(yy + sep)
                 .append(trayLimit);        
        return resultStr.toString();
    }
    
    private void setTime(String aTime) {
        if (aTime != null)
            this.time = Long.parseLong(aTime);
    }
    
    private void setIp(String aIp) {
        this.ip = ip2long(aIp);
    }

    private void setChannel(String channelId) {
        this.channel = channelId == null ? "1" : channelId; 
    }

    private void setMac(String mac) {
        this.mac = macFormat(mac);
    }

    private void setVersion(String aVersion) {
        this.version = version2long(aVersion);
    }
    
    private void setArea() {
        this.area = ipParser.getAreaId(ip);
    }

    private void setIsp() {
        this.isp = ipParser.getIspId(ip);
    }
    
    private void setOS(String os ){
        this.OS = os == null ? "-" : os ;
    }
    
    private void setStartType(String startType){
        this.startType = "4";
        if(startType.equals("0") || startType.equals("2") || startType.equals("3") || startType.equals("1") )
            this.startType = startType;
        if(startType.equalsIgnoreCase("startbywindowstray"))
            this.startType = "1";
        if(startType.equalsIgnoreCase("startbywindows"))
            this.startType = "2";
        if(startType.equals(" "))
            this.startType = "0";
        if(startType.equalsIgnoreCase("startbyinstall") || startType.equalsIgnoreCase("startbyinstalltray"))
            this.startType = "3";
        if(startType.toLowerCase().contains("rmvb"))
            this.startType = "0";
        if(startType.toLowerCase().contains("mp4"))
            this.startType = "0";
        if(startType.toLowerCase().contains("fc!"))
            this.startType = "0";
        if(startType.toLowerCase().contains("fsp"))
            this.startType = "0";
        if(startType.toLowerCase().contains("mediadir"))
            this.startType = "0";
        if(startType.toLowerCase().contains("desktop"))
            this.startType = "0";
        if(startType.toLowerCase().contains("programgroup") || startType.equalsIgnoreCase("commonlink"))
            this.startType = "0";
        if(startType.toLowerCase().contains("quicklaunch"))
            this.startType = "0";
        if(startType.toLowerCase().contains("startmenu"))
            this.startType = "0";
    }
    
    private void setErrorCode(String errorCode) {
        this.errorCode = errorCode == null ? "-" : errorCode;
    }
    
    private void setHc(String hc) {
        this.hc = hc == null ? "-" : hc;
    }

    private void setAccelerate(String accelerate){
        this.accelerate = accelerate == null ? "0" : accelerate;
    }  
    
    private void setYy(String yy) {
        this.yy = yy == null ? "1" : yy;
    }
  
    private void setTrayLimit(String trayLimit) {
        this.trayLimit = trayLimit == null ? "0" : trayLimit;
    }
    
    private int length = 12; 
    private final static String sep = "\t";
    private long time   = 0;
    private long   ip   = 0;
    private String area = null;
    private String isp  = null;
    private String mac  = null;
    private long   version    = 0;
    private String channel    = null;
    private String OS         = null;
    private String startType  = null;
    private String errorCode  = null;
    private String hc         = null;
    private String yy         = "1";
    private String accelerate = "";
    private String trayLimit  = "0";
    private String date       = null;
    private IpParser ipParser = null;  
    
}
