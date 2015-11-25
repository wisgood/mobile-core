package com.bi.client.quality.enums;

public enum PlayHaltDetailEnum {
    /*
     * SID, SESSION ID
     * CLIENTIP, 客户端IP
     * TIMESTAMP, 接收上报时间(rt)：unix时间戳
     * PGID, package_id，每个客户端独立编号
     * PVS, 工作模式
     * DHC,　自由拖动导致卡总时间
     * DHT,　自由拖动导致卡总次数
     * HC, 卡的总次数
     * HT, 卡类别
     * HTA,　卡的总时间
     * IH, 任务infohash
     * MAC, MAC地址 
     * NT, NAT类型
     * TPT, 任务播放时间 
     * TT, 卡时平均下载速度
     * VV, 客户端版本,点分十进制
     * PROVINCE, 省ID
     * CITY, 城市ID
     * ISP, 运营商ID
     * 详情请见http://redmine.funshion.com/redmine/projects/data-analysis/wiki/Play_halt_detail
     */
    
    SID, CLIENTIP, TIMESTAMP, PGID, PVS, DHC, DHT, HC, HT, HTA, IH, MAC, NT, TPT, TT, VV, PROVINCE, CITY, ISP;
}
