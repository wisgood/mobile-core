package com.bi.client.quality.enums;

public enum InlinePageEnum {
    /*
     * SID, SESSION ID
     * CLIENTIP, 客户端IP
     * TIMESTAMP, 接收上报时间(rt)：unix时间戳
     * PGID, package_id，每个客户端独立编号
     * PVS, 工作模式
     * MAC, MAC地址 
     * SIP, 服务器IP
     * TU, 消耗时间
     * WERR,　错误代码
     * VV, 客户端版本,点分十进制
     * PROVINCE, 省ID
     * CITY, 城市ID
     * ISP, 运营商ID
     * 详情请见http://redmine.funshion.com/redmine/projects/data-analysis/wiki/Inline_page
     */
    
    SID, CLIENTIP, TIMESTAMP, PGID, PVS, MAC, SIP, TU, WERR, VV, PROVINCE, CITY, ISP;
}
