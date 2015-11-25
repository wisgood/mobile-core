package com.bi.common.logenum;

public enum MPVEnum {
/*	
  字段顺序	字段名	  	参数名称		类型		描述		获取方式		备注	原上报
	p1	protocol		日志协议版本	server	标记协议		http://stat.funshion.net/website/mpv?rprotocol=1
	p2	rprotocol		请求协议版本	前端			fck=138544832626ef7
	p3	time			日志记录时间	server			userid=0
	p4	ip	Internet Protocol		用户IP地址	server	标记IP		sid=1385609083372cd
	p5	clientFlag			www：1；fs：2；fsqq：3；移动web：4	前端			pvid=dd650b99-ec3f-481d-7d76-4f128f1bbe14
	p6	fck			时间戳+随机数	前端	标记唯一用户		config=index
	p7	mac			用户Mac地址	前端	标记安装用户		url=http://m.funshion.com/index
	p8	userid			登陆用户id，未登录为0	前端	标记登陆用户		referurl=
	//p9	fpc			策略、运营商和地域用户的地址，策略，isp信息	前端	标记登陆用户		
	p10	version			风行版本号	前端	标记安装版本		ta=0
	p11	sid	sessionid		当前会话超过30分钟生成新值	前端			ext=
	p12	pvid			页面ID，每次刷新页面生成一个新值（UUID算法）	前端	标记访问页		
	p13	config	F.config.ctrlname		页面唯一标识	前端	标记唯一页面类
	p14	url			当前url地址	前端	
	p15	referurl			当前url来源url	前端	
	p16	channelid			合作渠道id	前端	
	p17	vtime			页面请求耗时	前端		上报说明：
	p18	ext			可扩展字段（新版1；旧版0）	前端	标记新旧版	
	p19	useragent			用户操作系统、浏览器信息	server	
	p20	step			用户史来pv计数器，各自维护	前端	
	p21	sestep			本次session的pv计数器，各自维护	前端	
	p22	seidcount			用户史来session计数器，各自维护	前端	
	p23	ta			ucs用户分类	前端	
*/
	PROTOCOL, RPROTOCOL,CLIENT_FLAG, TIMESTAMP, IP,  FCK, MAC, USERID, VERSION, SESSIONID, 
	PVID, CONFIG, URL, REFERURL, QUDAO_ID, VTIME, EXT, USERAGENT, STEP, SESTEP, SEIDCOUNT,TA;
}
