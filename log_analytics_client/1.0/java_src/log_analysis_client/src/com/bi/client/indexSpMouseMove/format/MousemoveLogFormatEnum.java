package com.bi.client.indexSpMouseMove.format;

public enum MousemoveLogFormatEnum {
	/**
	 *hdfs路径：/dw/logs/client/format/indexSpMouseMove/
	 *
	 *1		DATE_ID
	 *2		PROVINCE_ID
	 *3		ISP_ID
	 *4		MAC
	 *5		LONGIP
	 *6		TIMESTAMP:时间戳
	 *7		FCK:标记唯一用户	
	 *8		USER_ID:用户登录ID，未登录为0
	 *9		FPC:策略—运营商-地域
	 *10	CHANNEL_ID:渠道号
	 */
	TIMESTAMP,IP,MAC,USER_ID,FPC,CHANNEL_ID,FCK;
}
