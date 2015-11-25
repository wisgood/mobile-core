package com.bi.client.quality.enums;

public enum PlayHaltDetailFormatEnum {
    /*
     * DATE_ID,日期ID
     * HOUR_ID,小时ID
     * VERSION_ID,版本ID
     * PROVINCE_ID,省ID
     * CITY_ID,城市ID
     * ISP_ID,运营商ID
     * MAC_FORMAT, 经过验证的MAC地址
     * HC_FORMAT, 卡次数,验证为非负整数
     * HTA_FORMAT,　卡时间,验证为非负整数
     * HT_FORMAT,　卡类别,验证只有两种(1-边看边下；2-先下后看)
     * DHC_FORMAT, 自由拖动导致卡总时间,验证为非负整数
     * DHT_FORMAT, 自由拖动导致卡总时间,验证为非负整数
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
    DATE_ID, HOUR_ID, VERSION_ID, PROVINCE_ID, CITY_ID, ISP_ID, MAC_FORMAT, HC_FORMAT, HTA_FORMAT, HT_FORMAT, DHC_FORMAT, DHT_FORMAT,
    SID, CLIENTIP, TIMESTAMP, PGID, PVS, DHC, DHT, HC, HT, HTA, IH, MAC, NT, TPT, TT, VV, PROVINCE, CITY, ISP;
}
