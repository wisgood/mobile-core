package com.bi.client.quality.enums;

public enum DtfspEnum {
    /*
     * SID, SESSION ID
     * CLIENTIP, 客户端IP
     * TIMESTAMP, 接收上报时间(rt)：unix时间戳
     * PGID, package_id，每个客户端独立编号
     * PVS, 工作模式
     * MAC, MAC地址 
     * SIP, 下载fsp文件的IP地址
     * SPT, 下载fsp文件时间
     * LE, 下载fsp文件状态
     * URL, 下载fsp文件的URL地址
     * VV, 客户端版本,点分十进制
     * WPT, 下载JSON peer类型
     * PROVINCE, 省ID
     * CITY, 城市ID
     * ISP, 运营商ID
     * 详情请见http://redmine.funshion.com/redmine/projects/data-analysis/wiki/Dtfsp
     */
    
    SID, CLIENTIP, TIMESTAMP, PGID, PVS, MAC, SIP, SPT, LE, URL, VV, WPT, PROVINCE, CITY, ISP;
}
