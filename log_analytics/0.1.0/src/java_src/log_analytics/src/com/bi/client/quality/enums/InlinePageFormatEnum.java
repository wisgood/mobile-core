package com.bi.client.quality.enums;

public enum InlinePageFormatEnum {
    /*
     * DATE_ID,日期ID
     * HOUR_ID,小时ID
     * VERSION_ID,版本ID
     * PROVINCE_ID,省ID
     * CITY_ID,城市ID
     * ISP_ID,运营商ID
     * MAC_FORMAT, 经过验证的MAC地址
     * TU_FORMAT, 验证后的消耗时间
     * WERR_FORMAT, 验证后的消耗时间
     * IS_SIP,　验证服务器IP是否正确
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
    DATE_ID, HOUR_ID, VERSION_ID, PROVINCE_ID, CITY_ID, ISP_ID, MAC_FORMAT, TU_FORMAT, WERR_FORMAT, IS_SIP,
    SID, CLIENTIP, TIMESTAMP, PGID, PVS, MAC, SIP, TU, WERR, VV, PROVINCE, CITY, ISP;
}
