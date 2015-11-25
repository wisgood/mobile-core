/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: Pv2LogEnum.java 
* @Package com.bi.client.pv2.format 
* @Description: 对日志名进行处理
* @author limm
* @date 2013-9-10 上午10:49:07 
* @input:输入日志路径/2013-9-10
* @output:输出日志路径/2013-9-10
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.client.pv2.format;

/** 
 * @ClassName: Pv2LogEnum
 * @hdfsaPath: /dw/logs/web/origin/pv/2 
 * @Description: pv2日志的列格式说明
 * @author limm 
 * @date 2013-9-10 上午10:49:07  
 */
public enum Pv2LogEnum {
	/**
	 * 1	protocol	该协议的输出版本号，每个协议不管任何一部分作变动，该版本号都要升级跟踪
	 * 2	rprotocol	后端接收到的日志请求协议
	 * 3	timestamp	时间戳
	 * 4	ip			用户IP
	 * 5	clientFlag	www：1；fs：2；fsqq：3；移动web：4
	 * 6	fck			由js代码加入cookie的唯一标识，用以标识唯一用户
	 * 7	mac			mac地址，安装风行客户端的机器来获取
	 * 8	userid		登录用户注册id，如果未登录为0
	 * 9	fpc			策略、运营商和地域用户的地址，策略，isp信息
	 * 10	version		风行版本号
	 * 11	sid			当前会话ID，由js生成，算法跟fck类似，生命周期定义为30分钟
	 * 12	pvid		页面ID，每次刷新页面生成一个新值（UUID算法）
	 * 13 	config		页面唯一标示，页面分类
	 * 14	url			当前url地址
	 * 15	referurl	前链url
	 * 16	channelID	合作渠道id
	 * 17	vtime		页面请求耗时
	 * 18	ext			扩展字段pagetype
	 * 19	useragent	用户的操作系统、浏览器信息
	 * 20	step		格式：用户史来pv计数器，各自维护
	 * 21	sestep		格式：本次session的pv计数器，各自维护
	 * 22	seidcount	用户史来session计数器，各自维护
	 * 23	ta			格式ta|ucs，表示“ta策略|ucs用户分类”（同移动统一）
	 */
	PROTOCOL,RPROTOCOL,TIMESTAMP,IP,CLIENT_FLAG,FCK,MAC,USER_ID,FPC,VERSION,SID,PVID,CONFIG,URL,REFERURL,CHANNEL_ID,VTIME,EXT,USERAGENT,STEP,SESTEP,SEIDCOUNT,TA;
}
