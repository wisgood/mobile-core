package com.bi.client.quality.enums;

public enum PlayHaltFormatEnum {
    
    /*
     * DATE_ID,日期ID
     * HOUR_ID,小时ID
     * VERSION_ID,版本ID
     * PROVINCE_ID,省ID
     * CITY_ID,城市ID
     * ISP_ID,运营商ID
     * MAC_FORMAT, 经过验证的MAC地址
     * HC_FORMAT,　经过验证的卡次数
     * HTA_FORMAT, 经过验证的卡时间
     * SID, SESSION ID
     * CLIENTIP, 客户端IP
     * TIMESTAMP, 接收上报时间(rt)：unix时间戳
     * PGID, package_id，每个客户端独立编号
     * PVS, 工作模式
     * DCN, 用户拖动卡次数(drag choke num)
     * BS, 任务开始时滑窗起始位置(bitfiled start)
     * FPI, 任务开始时播放器第一次读取的piece idx, first piece idx
     * HC, 卡的总次数
     * HTA,　卡的总时间
     * IH, 任务infohash
     * MAC, MAC地址 
     * NHCN, 正常播放高速率卡次数(normal high rate choke num)
     * NLCN, 正常播放低速率卡次数(normal low rate choke num)
     * NT, NAT类型
     * TPT, 任务播放时间 
     * TT, 卡时平均下载速度
     * UDN, 用户拖动次数(user drag num)
     * UPN, 用户暂停次数(user pause num)
     * LPT, 最后一次卡时间(last pchoke time)
     * VV, 客户端版本,点分十进制
     * PROVINCE, 省ID
     * CITY, 城市ID
     * ISP, 运营商ID
     * 详情请见http://redmine.funshion.com/redmine/projects/data-analysis/wiki/Play_halt
     */
    
    DATE_ID, HOUR_ID, VERSION_ID, PROVINCE_ID, CITY_ID, ISP_ID, MAC_FORMAT, HC_FORMAT, HTA_FORMAT,
    SID, CLIENTIP, TIMESTAMP, PGID, PVS, DCN, BS, FPI, HC, HTA, IH, MAC, NHCN, NLCN, NT, TPT, TT, UDN, UPN, LPT, VV, PROVINCE, CITY, ISP;
}