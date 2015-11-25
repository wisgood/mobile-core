package com.bi.mobile.evideo.format.dataenum;

public enum EvideoFormatEnum {
	/**
	 *	设备类型(dev)：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号>
	  	mac地址(mac)：长度为16的大写字符串
      	app版本号(ver)：类ip地址的字符串
	  	网络类型(nt)：1—wifi，2--3g，3—其它，-1-无网络
	  	微视频类型(type)： 例 娱乐—entertainments
	  	短视频媒体id(videoid)：目前长度为5/6的字符串（具体数值由网站返回）
	  	渠道ID(sid):区分各个渠道商
	  	播放器类型(ptype)：android系统播放器：0；ffmpeg播放器：1；（android应用使用）
		上报时间(rt)：unix时间戳 ( iphone) 
		用户ip(iphoneip)： (iphone) 
		影片清晰度(cl):  1-tv,2-dvd,3-highdvd,4-superdvd
	 */
	DATE_ID, HOUR_ID, FLAG_ID, PLAT_ID, VERSION_ID, QUDAO_ID, CHANNEL_ID, CITY_ID, MAC_CODE, VID, VIDEONAME, PROVINCE_ID, ISP_ID,TIMESTAMP, IP, DEV, MAC, VER, NT, TYPE, VIDEOID, SID, PTYPE, RT, IPHONEIP, CL;


}
