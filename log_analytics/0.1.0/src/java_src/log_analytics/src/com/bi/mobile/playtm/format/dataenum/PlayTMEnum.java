package com.bi.mobile.playtm.format.dataenum;

public enum PlayTMEnum {
/**
 * 	设备类型(dev)：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号>
	设备mac地址(mac)：长度为16的大写字符串
	app版本号(ver)：类ip地址的字符串
	网络类型(nt)：1—wifi，2--3g，3—其它，-1-无网络
	任务infohash id(ih)：长度为40的sha1字符串
	播放起始位置(spos)：媒体文件的播放时间轴上的起始位置，单位：ms
	播放结束位置(epos)：媒体文件的播放时间轴上的结束位置，单位：ms
	实际观看时间(vtm)：在播放器界面停留的时间，单位：ms
	渠道ID(sid):区分各个渠道商
	播放总卡次数(pn): 播放时的总卡次数
	播放总卡时长(tu): 播放时的总卡时长，单位：ms
	播放器类型(ptype)：android系统播放器：0；ffmpeg播放器：1；（android应用使用）
	播放中断原因(pbre):退出播放器原因，0：播放结束，1：用户主动退出，
2：进入到后台退出，3：多剧集媒体用户切换退出，10：其他
	上报时间(rt)：unix时间戳 (ipad, iphone) 
	影片清晰度(cl):  1-tv,2-dvd,3-highdvd,4-superdvd

 */
	TIMESTAMP, IP, DEV, MAC, VER, NT, IH, SPOS, EPOS, VTM, SID, PN,TU,PTYPE,PBRE,RT,IPHONEIP,CL,FUDID,VT,TYPE,MID,EID,SN,ST;
}
