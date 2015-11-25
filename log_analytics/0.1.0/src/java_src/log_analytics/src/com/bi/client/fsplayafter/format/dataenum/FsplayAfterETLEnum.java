package com.bi.client.fsplayafter.format.dataenum;

public enum FsplayAfterETLEnum {
	/**
	*	
		时间戳(timestamp):
		用户ip(ip): 
		媒体ID(mid):
		用户ID号(uid):为0，未登陆；
		设备mac地址(mac)：长度为16的大写字符串
		任务infohash id(ih)：长度为32的sha1字符串
		用户唯一标识(fck)：长度为40的sha1字符串
     */
	
	DATE_ID,HOUR_ID,PLAT_ID,CHANNEL_ID,CITY_ID,MAC_CODE,MEDIA_ID,SERIAL_ID,PROVINCE_ID, ISP_ID, TIMESTAMP, IP, MID, UID, MAC, FCK, IH;

}