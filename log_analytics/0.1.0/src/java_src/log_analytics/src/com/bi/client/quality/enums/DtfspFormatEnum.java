package com.bi.client.quality.enums;

public enum DtfspFormatEnum {
    /*
     * DATE_ID,日期ID
     * HOUR_ID,小时ID
     * VERSION_ID,版本ID
     * PROVINCE_ID,省ID
     * CITY_ID,城市ID
     * ISP_ID,运营商ID
     * MAC_FORMAT, 经过验证的MAC地址
     * LE_FORMAT, 下载fsp文件状态, 验证正确性0,1,2,3,　失败后置为-1
     * SPT_FORMAT, 下载fsp文件时间, 验证正确性（非负整数）,失败后置为-1
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
     * WPT, 下载FSP peer类型
     * PROVINCE, 省ID
     * CITY, 城市ID
     * ISP, 运营商ID
     * 详情请见http://redmine.funshion.com/redmine/projects/data-analysis/wiki/Dtfsp
     */
    
    DATE_ID, HOUR_ID, VERSION_ID, PROVINCE_ID, CITY_ID, ISP_ID, MAC_FORMAT, LE_FORMAT, SPT_FORMAT,
    SID, CLIENTIP, TIMESTAMP, PGID, PVS, MAC, SIP, SPT, LE, URL, VV, WPT, PROVINCE, CITY, ISP;
}
