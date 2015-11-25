package com.bi.common.logenum;

public enum PushreachEnum {

    /**
     * 
     设备类型(dev)：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号> 
     * 设备mac地址(mac)：长度为16的大写字符串  app版本号(ver)：类ip地址的字符串 
     * 网络类型(nt)：1—wifi，2--3g，3—其它  消息类型(messagetype): 0---其他通知 1----通知栏通知
     * 2----桌面通知 3----通知栏和桌面通知都显示  消息ID(messageid): 推送信息管理系统中的消息id（可以为空） 
     * 类型(videotype):1--短视频，2--长视频3--直播 4--浏览器 5--其它  消息是否展示成功（ok）： 1----成功
     * 2-----管理后台数据异常 3----其他失败  CMD字段命令类型（cmd）：推送信息管理系统中的消息命令cmd 
     * 渠道ID(sid):区分各个渠道商
     */
    TIMESTAMP, IP, DEV, MAC, VER, NT, MESSAGETYPE, ID, VIDEOTYPE, OK, CMD, SID;
}
