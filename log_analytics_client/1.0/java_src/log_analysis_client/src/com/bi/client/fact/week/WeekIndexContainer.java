package com.bi.client.fact.week;

import java.util.ArrayList;

public class WeekIndexContainer {  
    
    public void setLoginUser(int loginUser) {
        this.loginUser += loginUser;
    }
 
    public void setNewUser(int newUser) {
        this.newUser += newUser;
    }

    public void setTotalOnlineLength(long totalOnlineLength) {
        this.totalOnlineLength += totalOnlineLength;
    }
    
    public void setAverageOnlineLength() {
        this.averageOnlineLength = loginUser == 0 ? 0 : (float)totalOnlineLength / loginUser;
    }
    
    public void setAverageUiOnlineLength() {
        this.averageUiOnlineLength = uiUser == 0 ? 0 : (float)totalUiOnlineLength / uiUser;
    }

    public void setBootUser(int bootUser) {
        this.bootUser += bootUser;
    }

    public void setWatchUser(int watchUser) {
        this.watchUser += watchUser;
    }
    
    public void setUiUser(int uiUser) {
        this.uiUser += uiUser;
    }

    public void setTotalUiOnlineLength(long uiOnlineLength) {
        this.totalUiOnlineLength += uiOnlineLength;
    }

    public void setInstallNum(int installNum) {
        this.installNum += installNum;
    }

    public void setValidInstallNum (int validInstallNum) {
        this.validInstallNum += validInstallNum;
    }

    public void setUninstallUser(int uninstallUser) {
        this.uninstallUser += uninstallUser;
    }

    public void setWatchLength(int watchLength) {
        this.watchLength += watchLength;
    }
    
    public void setAverageWatchLength() {
        this.averageWatchLength = watchUser == 0 ? 0 :  (float)watchLength / watchUser;
    }
    
    public void setAverageLoginUserWatchLength(){
        this.AverageLoginUserWatchLength = loginUser == 0 ? 0 : (float)watchLength / loginUser;
    }

    public void setTotalStartDates(int startDates) {
        this.totalStartDates += startDates;
    }
    
    public void setVerageStartDates(){
        this.averageStartDates =  bootUser == 0 ? 0 : (float)totalStartDates / bootUser;
    }
    
    public int getStartType(int Type){
        return startType[Type]; 
    }
    public void setStartType(int Type, int num){
        startType[Type] += num; 
    }
    
    public void addAllIndex(String value){
        String[] index = value.split(",");
        loginUser         += Integer.parseInt(index[0]);
        newUser           += Integer.parseInt(index[1]);
        totalOnlineLength += Long.parseLong(index[2]);
        bootUser          += Integer.parseInt(index[3]);
        totalStartDates   += Integer.parseInt(index[4]);
        startType[0]      += Integer.parseInt(index[5]);
        startType[1]      += Integer.parseInt(index[6]);
        startType[2]      += Integer.parseInt(index[7]);
        startType[3]      += Integer.parseInt(index[8]);
        installNum        += Integer.parseInt(index[9]);
        validInstallNum   += Integer.parseInt(index[10]);
        uninstallUser     += Integer.parseInt(index[11]);
        uiUser            += Integer.parseInt(index[12]);
        totalUiOnlineLength += Long.parseLong(index[13]);
        watchUser         += Integer.parseInt(index[14]);
        watchLength       += Long.parseLong(index[15]);
    }
    public String toString(){
        StringBuilder str = new StringBuilder();
        String sep = ","; 
        str.append(loginUser + sep)
           .append(newUser + sep)
           .append(totalOnlineLength + sep)
           .append(bootUser + sep)
           .append(totalStartDates + sep)
           .append(startType[0] + sep)
           .append(startType[1] + sep)
           .append(startType[2] + sep)
           .append(startType[3] + sep)
           .append(installNum + sep)
           .append(validInstallNum + sep)
           .append(uninstallUser + sep)
           .append(uiUser + sep)
           .append(totalUiOnlineLength + sep)
           .append(watchUser + sep)
           .append(watchLength);
        return str.toString();
    }
    
    public ArrayList<String> toResult(){
        ArrayList<String> indexArray = new ArrayList<String>();
        // 1 :  上线用户数 
        if(loginUser > 0)
            indexArray.add(1 + "\t" + loginUser);
        // 7 :  新增用户数
        if(newUser > 0)
            indexArray.add(7 + "\t" + newUser);
        // 5 : 在线时长
        if(totalOnlineLength > 0 ){
            indexArray.add(5 + "\t" + (float)totalOnlineLength/36000000);
        // 6 : 平均在线时长
            setAverageOnlineLength();
            indexArray.add(6 + "\t" + averageOnlineLength);
        }
        // 18 : 平均启动天数
        if(totalStartDates > 0){
            setVerageStartDates();
            indexArray.add(18 + "\t" + averageStartDates);
        }
        // 8: 启动用户数
        if( bootUser > 0)
            indexArray.add(8 + "\t" + bootUser);
        // 9 : _手动启动用户数
        if( startType[0] > 0)
            indexArray.add(9 + "\t" + startType[0]);
        // 10: _随开机启动并隐藏到托盘用户数
        if(startType[0] > 0)
            indexArray.add(10 + "\t" + startType[1]);
        // 11: _随机启动不隐藏到托盘用户数
        if(startType[2] > 0)
            indexArray.add(11 + "\t" + startType[2]);
        // 12: _安装完成后启动用户数
        if(startType[3] > 0)
            indexArray.add(12 + "\t" + startType[3]);
        // 13: 安装用户数
        if(installNum > 0)
            indexArray.add(13 + "\t" + installNum);
        // 14: 有效安装用户数
        if(validInstallNum > 0)
            indexArray.add(14 + "\t" + validInstallNum);
        // 15: 卸载用户数
        if(uninstallUser > 0)
            indexArray.add(15 + "\t" + uninstallUser);
        // 16: ui在线时长 (平均)
        if(totalUiOnlineLength > 0){
            setAverageUiOnlineLength();
            indexArray.add(16 + "\t" + averageUiOnlineLength);
        }
        // 17: 用户观看时长
        if(watchLength > 0 ){
            indexArray.add(17 + "\t" + (float)watchLength/36000000);
        // 21: 平均每用户观看时长
            setAverageLoginUserWatchLength();
            indexArray.add(21 + "\t" + AverageLoginUserWatchLength);
            
        // 22: 平均每观看用户观看时长
            setAverageWatchLength();
            indexArray.add(22 + "\t" + averageWatchLength);
        }
        // 20: 观看用户数
        if(watchUser > 0)
        indexArray.add(20 + "\t" + watchUser);
        
        return indexArray;
    }

    private int  loginUser = 0;  
    private int  newUser = 0; 
    private long totalOnlineLength = 0;
    private int  bootUser = 0;
    private int  totalStartDates =0 ;    
    private int[] startType = new int[5];
    private int  installNum  = 0;
    private int  validInstallNum = 0;
    private int  uninstallUser = 0;
    private int  uiUser = 0;
    private long totalUiOnlineLength = 0;
    private int  watchUser = 0;
    private long  watchLength = 0;
    private float averageWatchLength = 0;
    private float averageStartDates = 0;
    private float averageUiOnlineLength = 0;
    private float averageOnlineLength   = 0;
    private float AverageLoginUserWatchLength = 0;
}
