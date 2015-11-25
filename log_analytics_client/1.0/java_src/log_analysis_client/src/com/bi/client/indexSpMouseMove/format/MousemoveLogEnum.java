package com.bi.client.indexSpMouseMove.format;

public enum MousemoveLogEnum {
	/**
	 *hdfs路径：/dw/logs/web/origin/indexSpMouseMove/1/
	 *
	 *1		TIMESTAMP:时间戳
	 *2		IP:IP地址
	 *3		MAC:mac地址
	 *4		USER_ID:用户登录ID，未登录为0
	 *5		FPC:策略—运营商-地域
	 *6		CHANNEL_ID:渠道号
	 *7		FCK:标记唯一用户
	 */
	TIMESTAMP,IP,MAC,USER_ID,FPC,CHANNEL_ID,FCK;
}
