package com.bi.common.logenum;

public enum DbufferEnum {
    /**
     *     
                 设备类型(dev)：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号>
   设备mac地址(mac)：长度为16的大写字符串
   app版本号(ver)：类ip地址的字符串
   网络类型(nt)：1—wifi，2--3g，3—其它，-1-无网络
   任务infohash id(ih)：长度为40的sha1字符串
   server地址：缓冲所连接的server ip地址，ios—填“”，andriod—server ip地址
   缓冲是否成功(ok)：0—缓冲成功，-1—缓冲失败
   当前下载位置(dpos)：当前已经下载数据支持的最大可播放位置，单位：ms
   拖动起始位置(spos)：拖动前的播放位置，单位：ms
   当前缓冲位置(bpos)：媒体文件播放时间轴上的缓冲起始位置，单位：ms
   缓冲时间(btm)：缓冲成功表示实际缓冲时间，失败表示用户的等待时间，单位：ms
   缓冲期间平均下载速度(drate)：单位KB/s，default： -1
   渠道ID(sid):区分各个渠道商
   播放器类型(ptype)：android系统播放器：0；ffmpeg播放器：1；（android应用使用）
   上报时间(rt)：unix时间戳 (ipad, iphone) 
   用户ip(ip)： (iphone) 
   影片清晰度(cl):  1-tv,2-dvd,3-highdvd,4-superdvd
     */
    TIMESTAMP, IP, DEV, MAC, VER, NT, IH, SERVERIP, OK,DPOS,SPOS,BPOS,BTM,DRATE,SID,PTYPE,RT,IPHONEIP,CL;
}
