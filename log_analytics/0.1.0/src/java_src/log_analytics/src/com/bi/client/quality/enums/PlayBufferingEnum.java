package com.bi.client.quality.enums;

public enum PlayBufferingEnum {
    /*
     * SID, SESSION ID
     * CLIENTIP, 客户端IP
     * TIMESTAMP, 接收上报时间(rt)：unix时间戳
     * PGID, package_id，每个客户端独立编号
     * PRN, 上报中的Peer Number, 后添加字段，之前版本置为undef
     * PVS, 工作模式
     * BT, 缓冲时间, 单位ms
     * IH, 任务infohash
     * MAC, MAC地址
     * NT, NAT类型
     * OK,　缓冲是否完成标志
     * PRN, 上报中的Peer Number, 后添加字段，之前版本置为undef
     * TT, 当前下载速率
     * APCT, first act peer cost time
     * BTLT, 任务下载种子耗时
     * FRCT, first request cost time
     * TPCT,  first temp peer cost time
     * PT, 是否为直播
     * VV, 客户端版本,点分十进制
     * PROVINCE, 省ID
     * CITY, 城市ID
     * ISP, 运营商ID
     * 详情请见http://redmine.funshion.com/redmine/projects/data-analysis/wiki/Play_buffering
     */
    
    SID, CLIENTIP, TIMESTAMP, PGID, PRN, PVS, BT, IH, MAC, NT, OK, SDN, TT, APCT, BTLT, FRCT, TPCT, PT, VER, PROVINCE, CITY, ISP;

}
