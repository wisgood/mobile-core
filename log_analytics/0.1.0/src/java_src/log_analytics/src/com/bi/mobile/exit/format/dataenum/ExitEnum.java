package com.bi.mobile.exit.format.dataenum;

public enum ExitEnum {
	/**
	 * 	设备类型(dev)：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号>
	设备mac地址(mac)：长度为16的大写字符串
	app版本号(ver)：类ip地址的字符串
	网络类型(nt)：1—wifi，2--3g，3—其它 ，-1-无网络
	使用时长（usetm）：单位ms
	退出时任务数量(tn)：default -1
	渠道ID(sid):区分各个渠道商
	上报时间(rt)：unix时间戳 (ipad, iphone) 

	 */
	TIMESTAMP, IP, DEV, MAC, VER, NT, USETM,TN, SID, RT,IPHONEIP,FUDID;
}
