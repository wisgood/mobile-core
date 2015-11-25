package com.bi.client.fsplayafter.format.dataenum;

public enum FsplayAfterEnum {
	/**
	*	
		时间戳(timestamp):
		用户ip(ip): 
		媒体ID(mid):
		用户ID号(uid):为0，未登陆；
		设备mac地址(mac)：长度为16的大写字符串
		用户唯一标识(fck)：长度为32的sha1字符串
		任务infohash id(ih)：长度为40的sha1字符串
     */
	TIMESTAMP, IP, MID, UID, MAC, FCK, IH; 
}
