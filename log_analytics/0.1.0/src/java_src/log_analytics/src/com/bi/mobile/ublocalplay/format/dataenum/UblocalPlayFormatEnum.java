package com.bi.mobile.ublocalplay.format.dataenum;

public enum UblocalPlayFormatEnum {
	/**
	*	
		设备类型(dev)：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号>
		设备mac地址(mac)：长度为16的大写字符串
		app版本号(ver)：类ip地址的字符串
		网络类型(nt)：1—wifi，2--3g，3—其它，-1-无网络
		已下载完成百分比(per): 0~100, default:-1(不能获取情况下)
		是否播放成功(ok)：1-成功0-失败 default:-1(未知) 
		渠道ID(sid):区分各个渠道商


		媒体ID(mid):
		分集IDf(eid):长度为32位
		任务infohash id(ih)：长度为32的sha1字符串
		微视频ID(vid):
		视频类型(vt):1-长视频，2-微视频
     */
	DATE_ID,HOUR_ID,PLAT_ID,VERSION_ID,QUDAO_ID,CHANNEL_ID,CITY_ID,MAC_CODE,MEDIA_ID,SERIAL_ID,PROVINCE_ID, ISP_ID, TIMESTAMP, IP, DEV, MAC, VER, NT, PER, OK, SID, MID, EID, IH, VID, VT;

}