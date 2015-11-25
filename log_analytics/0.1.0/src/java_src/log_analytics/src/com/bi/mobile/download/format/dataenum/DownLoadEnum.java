package com.bi.mobile.download.format.dataenum;

public enum DownLoadEnum {
/**
 *  设备类型(dev)：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号>
	设备mac地址(mac)：长度为16的大写字符串
	app版本号(ver)：类ip地址的字符串
	网络类型(nt)：1—wifi，2--3g，3—其它，-1-无网络
	媒体id(mid)：目前长度为5的字符串（具体数值由网站返回）
	任务infohash id(ih)：长度为40的sha1字符串
	渠道ID(sid):区分各个渠道商
	上报时间(rt)：unix时间戳 (ipad, iphone) 
	用户ip(ip)： (iphone) 
	影片清晰度(cl):  1-tv,2-dvd,3-highdvd,4-superdvd

 */
	TIMESTAMP, IP, DEV, MAC, VER, NT, MID, IH, SID, RT,IPHONEIP,CL,FUDID,MESSAGEID;
	
	
	
}
