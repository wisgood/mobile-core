package com.bi.mobilequality.bootstrap.format.dataenum;

public enum BootStrapEnum {
    /*
     * 设备类型(dev)：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号> 
     * 设备mac地址(mac)：长度为16的大写字符串（待确认）  app版本号(ver)：类ip地址的字符串 
     * 网络类型(nt)：1—wifi，2--3g，3—其它 ，-1—无网络  
     * 启动方式（btype）:* 0—其它启动；1—手动启动；2—ios平台：推送启动
     * ，android平台：调用播放器播放本地文件；3—ios平台：有角标启动上报（其它启动不包括有角标启动上报
     * ），android平台：推送通知栏启动；4
     * —android平台：后台下载进入下载管理界面；5—android平台：网页调起app；6—android平台
     * ：按home键应用进入后台后再次回到前台
     * ；7—android平台：推送桌面弹窗启动；8—android平台：本地通知启动；9–android平台：通过引入第三方push
     * sdk创建的通知栏启动 10 –android平台：通过引入第三方push sdk创建的桌面弹窗启动 
     * 启动耗时（btime）：从点击到主框架加载完毕耗时，单位：ms  
     * 启动是否成功（ok）：1—成功，-1—其它—错误代码 
     * 屏幕分辨率（sr）：屏幕分辨率，N*M  
     * 设备内存空间（mem）：单位MB 
     *  设备存储空间（tdisk）：单位MB 
     * 设备剩余空间(fdisk)：单位MB  
     * 渠道ID(sid):区分各个渠道商  
     * 启动时间戳（rt）：unix时间戳 (ipad, iphone)
     * 是否越狱（broken）： (iphone) 
            设备IMEI（imei）: 设备IMEI号 (aphone，apad)

     */
    TIMESTAMP, IP, DEV, MAC, VER, NT, BTYPE, BTIME, OK, SR, MEM, TDISK, FDISK, SID, RT,BROKEN, IMEI;

}
