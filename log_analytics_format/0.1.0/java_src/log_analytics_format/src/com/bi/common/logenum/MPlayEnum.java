package com.bi.common.logenum;

public enum MPlayEnum {
/*	
	字段顺序	字段名	参数名称	类型	描述	获取方式	备注
	p1	protocol			日志协议版本	server	标记协议
	p2	rprotocol			请求协议版本	前端	
	p3	time			日志记录时间	server	
	p4	ip	Internet Protocol		用户IP地址	server	标记IP
	p5	clientFlag			www：1；fs：2；fsqq：3；移动web：4	前端	
	p6	fck			时间戳+随机数	前端	标记唯一用户
	p7	mac			用户Mac地址	前端	标记安装用户
	p8	userid			登陆用户id，未登录为0	前端	标记登陆用户
	//p9	fpc			由js代码加入cookie的唯一标识，用以标识唯一用户		
	p10	version			风行版本号	前端	标记安装版本
	p11	sid	sessionid		当前会话超过30分钟生成新值	前端	
	p12	pvid			页面ID，每次刷新页面生成一个新值	前端	标记访问页
	p13	config	F.config.ctrlname		页面唯一标识	前端	标记唯一页面类
	p14	url			当前url地址	前端	
	p15	referurl			当前url来源url	前端	
	p16	channelid			渠道id	前端	
	p17	mediatpye			格式：视频类型(媒体/微视频/UGC)|媒体类型|题材|标题或关联媒体|	前端	
	p18	target			格式：媒体id|种子id|专辑或标签id|微视频id|（无时标“0”）	前端	
	p19	hashid			hashid（ 和点击相关的target请求页面）	前端	
	p20	vvid			本次播放id，每次播放时生成一个新值(包括重播)	前端	
	p21	lian			连播上报：连播类型_联播次数 （连播类型："0"不连播"1"连播）	前端	
	p22	platform			站内外播放标识（0站内 1 站外其他 2腾讯微博 3 新浪微博 4开心网 5百度贴吧  6QQ空间）	前端	
	p23	videolength			节目时长(单位：秒)	前端	#REF!
	p24	format			当前播放码流|支持最高播放码流	前端	
	p25	ext			可扩展字段（新版1；旧版0）	前端	标记新旧版
	p26	step			用户史来play计数器，各自维护	前端																																																																																																										
	p27	playstep			本次session的play计数器，各自维护	前端	
	p28	playidcount			用户史来session计数器，各自维护	前端	
	p29	useragent			用户操作系统、浏览器信息	server	
*/
	
	PROTOCOL, RPROTOCOL, TIMESTAMP, IP, CLIENT_FLAG, FCK, MAC, USERID, VERSION, SESSION_ID, 
	PVID, CONFIG, URL, REFERURL, QUDAO_ID, MEDIATYPE, TARGET, HASHID, VVID, LIAN,
	PLATFORM, VIDEOLENGTH, FMT, EXT, STEP, SESTEP, SEIDCOUNT,URERAGENT;

}
