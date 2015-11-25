/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: Pv2LogFormatEnum.java 
* @Package com.bi.client.pv2.format 
* @Description: 对日志名进行处理
* @author limm
* @date 2013-9-10 下午3:30:16 
* @input:输入日志路径/2013-9-10
* @output:输出日志路径/2013-9-10
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.client.pv2.format;

/** 
 * @ClassName: Pv2LogFormatEnum 
 * @Description: pv2日志Format之后的列说明
 * @hdfsPath：/dw/logs/client/format/pv2 
 * @author limm 
 * @date 2013-9-10 下午3:30:16  
 */
public enum Pv2LogFormatEnum {
	/**
	 * 1	DATE_ID
	 * 2	HOUR_ID
	 * 3	PROVINCE_ID
	 * 4	ISP_ID
	 * 5	VERSION_ID
	 * 6	MAC			不为(null,""," "),默认为"-"
	 * 7	protocol	不为(null,""," "),默认为"-"
	 * 8	rprotocol	不为(null,""," "),默认为"-"
	 * 9	timestamp	时间戳,默认为0
	 * 10	LONG_IP		用户IP，默认为0
	 * 11	fck			不为(null,""," "),默认为"-"
	 * 12	userid		不为(null,""," "),默认为"-"
	 * 13	fpc			策略、运营商和地域用户的地址，策略，isp信息
	 * 14	sid			当前会话ID，由js生成，算法跟fck类似，生命周期定义为30分钟
	 * 15	pvid		页面ID，每次刷新页面生成一个新值（UUID算法）
	 * 16 	config		页面唯一标示，页面分类
	 * 17	url			当前url地址
	 * 18	referurl	前链url
	 * 19	channelID	合作渠道id
	 * 20	vtime		页面请求耗时
	 * 21	ext			扩展字段pagetype
	 * 22	useragent	用户的操作系统、浏览器信息
	 * 23	step		格式：用户史来pv计数器，各自维护
	 * 24	sestep		格式：本次session的pv计数器，各自维护
	 * 25	seidcount	用户史来session计数器，各自维护
	 * 26	ta			格式ta|ucs，表示“ta策略|ucs用户分类”（同移动统一）
	 */
	DATE_ID,HOUR_ID,PROVINCE_ID,ISP_ID,VERSION_ID,MAC,PROTOCOL,RPROTOCOL,TIMESTAMP,LONGIP,FCK,USER_ID,FPC,SID,PVID,CONFIG,URL,REFERURL,CHANNEL_ID,VTIME,EXT,USERAGENT,STEP,SESTEP,SEIDCOUNT,TA;
}
