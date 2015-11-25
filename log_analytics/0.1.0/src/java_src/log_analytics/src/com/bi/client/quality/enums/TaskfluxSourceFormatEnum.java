package com.bi.client.quality.enums;

public enum TaskfluxSourceFormatEnum {
    /*
     * DATE_ID,日期ID
     * HOUR_ID,小时ID
     * VERSION_ID,版本ID
     * PROVINCE_ID,省ID
     * CITY_ID,城市ID
     * ISP_ID,运营商ID
     * MAC_FORMAT, 经过验证的MAC地址
     * MS_FLUX, MS流量,验证是否为非负整数,若否则置为0
     * HIDDEN_MS_FLUX, 隐藏MS流量,验证是否为非负整数,若否则置为0
     * NORMAL_PEER_FLUX, 正常Peer流量,验证是否为非负整数,若否则置为0
     * SID, SESSION ID
     * CLIENTIP, 客户端IP
     * TIMESTAMP, 接收上报时间(rt)：unix时间戳
     * PGID, package_id，每个客户端独立编号
     * PVS, 工作模式
     * APN, 种子数
     * HMSF, Hidden ms流量
     * IH, 任务infohash
     * MAC, MAC地址 
     * MSF, Ms流量
     * NRF, normal peer流量
     * AN, 活动peer数 
     * TDR, 任务下载速率
     * VV, 客户端版本,点分十进制
     * PROVINCE, 省ID
     * CITY, 城市ID
     * ISP, 运营商ID
     * 详情请见http://redmine.funshion.com/redmine/projects/data-analysis/wiki/Taskflux_source
     */
    DATE_ID, HOUR_ID, VERSION_ID, PROVINCE_ID, CITY_ID, ISP_ID, MAC_FORMAT, MS_FLUX, HIDDEN_MS_FLUX, NORMAL_PEER_FLUX,
    SID, CLIENTIP, TIMESTAMP, PGID, PVS, APN, HMSF, IH, MAC, MSF, NRF, AN, TDR, VV, PROVINCE, CITY, ISP;
}
