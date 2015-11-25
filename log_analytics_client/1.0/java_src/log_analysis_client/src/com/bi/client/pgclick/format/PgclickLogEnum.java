package com.bi.client.pgclick.format;

public enum PgclickLogEnum {
	/**
	 *hdfs路径：/dw/logs/web/origin/pgclick/
	 * 
	 *1		PROTOCOL:日志协议版本
	 *2		RPROTOCOL:请求协议版本
	 *3		TIMESTAMP:时间戳
	 *4		IP:IP
	 *5		CLIENT_FLAG:www:1,fs:2,fsqq:3,移动web：4
	 *6		FCK:标记唯一用户
	 *7		MAC：Mac地址
	 *8		USER_ID:登陆用户ID,未登录为0
	 *9		fpc:策略、运营商和地域用户的地址，策略，isp信息
	 *10	VERSION:风行版本号
	 *11	SID:客户端启动时生成的ID，每次 会话重新生成一个          
	 *12	PVID:同一页面时与PV上报中相同。每次刷新页面生成一个新值
	 *13	CONFIG: 页面唯一标示，页面分类
	 *14	URL:当前URL地址
	 *15	REFERURL:当前URL来源url
	 *16	CHANNEL_ID:合作渠道ID
	 *17	BLOCK:点击的页面位置
	 *18	SCREENW:屏幕宽
	 *19	SCREENH:屏幕高
	 *20	BROWSERW:浏览器宽
	 *21	BROWSERH:浏览器高
	 *22	BROWSERPX:点击距离浏览器中间线内容区域的横向坐标，左侧为负
	 *23	BROWSERPY:点击距离浏览器中间线内容区域的纵向坐标
	 *24	PAGEPX:点击距离页面中间线内容区域的横向坐标，左侧为负
	 *25	PAGEPY:点击距离页面中间线内容区域的纵向坐标
	 *26	EXT:  扩展字段，turnurl=?&（key=value）（turnurl表示点击链接url）  
	 *27	USERAGENT:用户的操作系统、浏览器信息                                                                                                                  
	 */
	PROTOCOL,RPROTOCOL,TIMESTAMP,IP,CLIENT_FLAG,FCK,MAC,USER_ID,FPC,VERSION,SID,PVID,CONFIG,URL,REFERURL,CHANNEL_ID,BLOCK,SCREENW,SCREENH,BROWSERW,BROWSERH,BROWSERPX,BROWSERPY,PAGEPX,PAGEPY,EXT,USERAGENT;
}
