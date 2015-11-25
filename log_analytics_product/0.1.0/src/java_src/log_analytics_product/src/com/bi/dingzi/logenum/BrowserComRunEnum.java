package com.bi.dingzi.logenum;

public enum BrowserComRunEnum {
    
    /**
     * 
     * "rprotocol", # 日志请求协议版本号，由前端发送，表明前端发送的版本号 "
     * category", # 浏览器组件的类别，0=ie-bho;1=ie-activex;2=ff-extension;3=ff-plugin 
     * "name", # 组件名称
     * "version", # 组件版本 
     * "mac", # 本机mac地址 
     * "guid", # 计算机计算出来的用户标识，Globally Unique Identifier（全球唯一标识符） 
     * "broname", # 浏览器名称 
     * "broversion", # 浏览器版本 
     * "suc", #是否启动或拉起成功，1表示启动成功，0表示启动失败，2表示拉起成功，3表示拉起失败 
     * "url", #拉起风行客户端或钉子时浏览器的url（启动时为空） 
     * "type", # 0拉客户端，1拉钉子（启动时为空） 
     * "stratery", #拉起策略，1为需要拉起，0为不需要拉起（启动时为空）
     * 
     * 
     * 
     * 
     */
    PROTOCOL,RPROTOCOL,TIME,IP,CATEGORY, NAME, VERSION, MAC, GUID, BRONAME, BROVERSION, SUC, URL, TYPE,STRATERY;

}
