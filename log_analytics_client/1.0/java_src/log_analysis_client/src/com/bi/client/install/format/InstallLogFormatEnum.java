package com.bi.client.install.format;

public enum InstallLogFormatEnum {
	/**
	 * 1	DATE_ID:日期
	 * 2	VERSION_ID:版本
	 * 3	PROVINCE_ID:省份
	 * 4	MAC:
	 * 5	ISP_ID:
	 * 6	CHANNEL_ID:渠道ID
	 * 7	INSTALL_TYPE:安装类型，取值为（first|update|replace|unknown）
	 * 8	IS_AUTO:是否自启动
	 * 9	PS:pps影视，0为无，1为有
	 * 10	THD:迅雷看看，0为无，1为有
	 * 11	PL:PPTV影视，0为无，1为有
	 * 12	QY:奇异，0为无，1为有
	 * 13	PP:皮皮影视，0为无，1为有
	 * 14	LT:乐视，0为无，1为有
	 * 15	QL:腾讯视频，0为无，1为有
	 * 16 	SV:搜狐视频，0为无，1为有
	 * 17	BD:百度影音，0为无，1为有
	 * 18	QB:快播，0为无，1为有
	 * 19	BQ:暴风，0为无，1为有
	 * 20	AL:Alexa统计，0为无，1为有
	 * 21	IU:艾瑞统计，0为无，1为有
	 * 22 	LONGIP
	 * 23	TIMESTAMP:
	 */
	DATE_ID,VERSION_ID,PROVINCE_ID,MAC,ISP_ID,CHANNEL_ID,INSTALL_TYPE,IS_AUTO,PS,THD,PL,QY,PP,LT,QL,SV,BD,QB,BQ,AL,IU,LONGIP,TIMESTAMP;
}
