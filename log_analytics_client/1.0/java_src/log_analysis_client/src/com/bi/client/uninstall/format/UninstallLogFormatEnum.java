package com.bi.client.uninstall.format;

public enum UninstallLogFormatEnum {
	/**
	 * 1 	DATE_ID
	 * 2	VERSION_ID
	 * 3	PROVINCE_ID
	 * 4	MAC
	 * 5 	ISP_ID
	 * 6	CHANNEL_ID
	 * 7	RECORD_ID:
	 * 8	RUN_COUNT:风行启动次数
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
	 * 20	MODIFY_HISTORY:版本修改历史
	 * 21	TASK_COUNT:任务数
	 * 22	LAST_CRASH:上次崩溃时间
	 * 23	MAX_DOWNLOAD:最大下载速度
	 * 24	TOTAL_HIGH:总的下载量高32位数值
	 * 25	TOTAL_LOW:总的下载量剩余数值
	 * 26	LONGIP
	 * 27	TIMESTAMP
	 */
	DATE_ID,VERSION_ID,PROVINCE_ID,MAC,ISP_ID,CHANNEL_ID,RECORD_ID,RUN_COUNT,PS,THD,PL,QY,PP,LT,QL,SV,BD,QB,BQ,MODIFY_HISTORY,TASK_COUNT,LAST_CRASH,MAX_DOWNLOAD,TOTAL_HIGH,TOTAL_LOW,LONGIP,TIMESTAMP;
}
