package com.bi.client.quality.enums;

public enum PlayerBuffEnum {
    /*
     * SID, SESSION ID
     * CLIENTIP, 客户端IP
     * TIMESTAMP, 接收上报时间(rt)：unix时间戳
     * PGID, package_id，每个客户端独立编号
     * PVS, 工作模式
     * BS, Buffer Size
     * IH, 任务infohash
     * MAC, MAC地址 
     * VV, 客户端版本,点分十进制
     * PROVINCE, 省ID
     * CITY, 城市ID
     * ISP, 运营商ID
     * 详情请见http://redmine.funshion.com/redmine/projects/data-analysis/wiki/Play_buff
     */
    
    SID, CLIENTIP, TIMESTAMP, PGID, PVS, BS, IH, MAC, VV, PROVINCE, CITY, ISP;
}
