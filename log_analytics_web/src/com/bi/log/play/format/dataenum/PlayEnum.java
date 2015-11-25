package com.bi.log.play.format.dataenum;

public enum PlayEnum {
	/**
	 * 1    protocol: 日志协议版本
       2    rprotocol: 请求协议版本
	   3    timestamp: 日志记录时间
       4    ip: 用户IP地址
       5    fck: 时间戳+随机数
       6    mac: 用户Mac地址
       7    userid: 登陆用户id，未登录为0
       8    fpc: 策略、运营商和地域用户的地址，策略，isp信息
       9    version: 风行版本号
       10   sessionid: 当前会话超过30分钟生成新值
       11   pvid: 页面ID，每次刷新页面生成一个新值
       12   config: 页面唯一标识
       13   url: 当前url地址
       14   referurl: 当前url来源url
       15   ispid: 渠道id
	   16	mediatype:媒体类型 格式：x|x|x|x。视频类型(媒体/微视频/UGC/直播)|媒体类型|题材|标题, eg:media|cartoon|日本,悬疑,青春|名侦探柯南
	   17	target:媒体ID 格式：x|x|x|x。媒体id|种子id|(专辑/标签id)|微视频id。
	   18   hashid:种子ID
	   19   vvid:播放ID
	   20   lian:连播上报 是否连播|联播次数。
	   21   platform:站内外播放标识(英文标识).
	   22   videolength:节目时长
	   23   fmt:码流 格式：当前播放码流|支持最高播放码流。
	   24   pagetype扩展字段 格式：key=value
       25   step: 用户史来play计数器，各自维护
       26   sestep: 本次session的play计数器，各自维护
       27   seidcount: 用户史来session计数器，各自维护
	 */
	PROTOCOL, RPROTOCOL, TIMESTAMP, IP, FCK, MAC, USERID, FPC, VERSION, SESSIONID, 
	PVID, CONFIG, URL, REFERURL, ISPID, MEDIATYPE, TARGET, HASHID, VVID, LIAN,
	PLATFORM, VIDEOLENGTH, FMT, PAGETYPE, STEP, SESTEP, SEIDCOUNT;

}
