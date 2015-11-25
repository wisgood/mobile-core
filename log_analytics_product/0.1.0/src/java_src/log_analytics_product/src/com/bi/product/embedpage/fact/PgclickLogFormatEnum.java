package com.bi.product.embedpage.fact;

public enum PgclickLogFormatEnum {
	/**
	 *hdfs路径：/dw/logs/client/format/pgclick/
	 *
	 *1		DATE_ID
	 *2		HOUR_ID
	 *3		PROVINCE_ID
	 *4		ISP_ID
	 *5		VERSION:风行版本号
	 *6		MAC：Mac地址	 
	 *7		PROTOCOL:日志协议版本
	 *8		RPROTOCOL:请求协议版本
	 *9		LONGIP
	 *10	TIMESTAMP:时间戳
	 *11	FCK:标记唯一用户
	 *12	USER_ID:登陆用户ID,未登录为0
	 *13	FPC:策略、运营商和地域用户 的地址，策略，isp信息，客户端为空
	 *14	SID:客户端启动时生成的ID，每次 会话重新生成一个          
	 *15	PVID:同一页面时与PV上报中相同。每次刷新页面生成一个新值
	 *16	CONFIG: 页面唯一标示，页面分类
	 *17	URL:当前URL地址
	 *18	REFERURL:当前URL来源url
	 *19	CHANNEL_ID:合作渠道ID
	 *20	BLOCK:点击的页面位置
	 *21	SCREENW:屏幕宽
	 *22	SCREENH:屏幕高
	 *23	BROWSERW:浏览器宽
	 *24	BROWSERH:浏览器高
	 *25	BROWSERPX:点击距离浏览器中间线内容区域的横向坐标，左侧为负
	 *26	BROWSERPY:点击距离浏览器中间线内容区域的纵向坐标
	 *27	PAGEPX:点击距离页面中间线内容区域的横向坐标，左侧为负
	 *28	PAGEPY:点击距离页面中间线内容区域的纵向坐标
	 *29	EXT:  扩展字段，turnurl=?&（key=value）（turnurl表示点击链接url）  
	 *30	USERAGENT:用户的操作系统、浏览器信息                                                                                                                  */
	DATE_ID,HOUR_ID,PROVINCE_ID,ISP_ID,VERSION_ID,MAC,PROTOCOL,RPROTOCOL,LONGIP,TIMESTAMP,FCK,USER_ID,FPC,SID,PVID,CONFIG,URL,REFERURL,CHANNEL_ID,BLOCK,SCREENW,SCREENH,BROWSERW,BROWSERH,BROWSERPX,BROWSERPY,PAGEPX,PAGEPY,EXT,USERAGENT;
}
