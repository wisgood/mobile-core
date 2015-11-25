package com.bi.baidubrower.format;

import com.bi.baidubrower.util.*;

public class BrowerLog {
    
    public static enum RawLogOrder{
        TIMESTAMP, IP, FCK, INSTALLTIME, DAYTIME, TIMEDAY, COMPUSER
    }
    
    public static enum FormatLogOrder{
        TIMESTAMP,IP,FCK, INSTALLDATE, INSTALLTIME, DAYTIME, TIMEDAY,COMUSER
    }
    
    BrowerLog(int length) throws Exception{
        if( length != 7 )
            throw new Exception("Fields not match");
    }
    
    
    public void setFileds(String[] fields){
        setTime(fields[RawLogOrder.TIMESTAMP.ordinal()]);
        setIp(fields[RawLogOrder.IP.ordinal()]);
        setFck(fields[RawLogOrder.FCK.ordinal()]);
        setInstallTime(fields[RawLogOrder.INSTALLTIME.ordinal()]);
        setDayTime(fields[RawLogOrder.DAYTIME.ordinal()]);
        setTimeDay(fields[RawLogOrder.TIMEDAY.ordinal()]);
        setCompUser(fields[RawLogOrder.COMPUSER.ordinal()]);        
    }
    
    public String toString(){
        StringBuilder result = new StringBuilder();
        result.append(timeStamp + sep)
              .append(ip + sep)
              .append(fck + sep)
              .append(installDate + sep)
              .append(installTime + sep)
              .append(dayTime + sep)
              .append(timeDay + sep)
              .append(compUser);
        return result.toString();
    }
    

    private void setCompUser(String aCompUser) {
        compUser =  aCompUser == null ? "0" : aCompUser;
        
    }

    private void setTimeDay(String aTimeDay) {
        timeDay =  aTimeDay == null ? "0" : aTimeDay; 
    }

    private void setDayTime(String aDayTime) {
        dayTime = aDayTime == null ? "0" : aDayTime;       
    }

    private void setInstallTime(String aTime) {
        installTime = aTime == null ? "0" : aTime;
        installDate = TimeFormat.formatTimestamp(aTime).get("DATE_ID");
    }

    private void setFck(String aFck) {
        fck = aFck == null ? "0" : aFck;
    }

    private void setIp(String aIp) {
        ip = aIp == null ? "0" : aIp;
    }

    private void setTime(String aTime) {
        timeStamp = aTime == null ? "0" : aTime;
    }



    private final static String sep = "\t";
    private String timeStamp;
    private String ip;
    private String fck;
    private String installTime;
    private String installDate;
    private String dayTime; 
    private String timeDay;
    private String compUser;   
}
