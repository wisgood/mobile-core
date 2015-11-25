package com.bi.common.logenum;

public enum PvEnum {
	/**
	 * 1    protocol: 日志协议版本
       2    rprotocol: 请求协议版本
	   3    timestamp: 日志记录时间
       4    ip: 用户IP地址
     / 5    clientflag: 客户端网站区分标志位
       5    fck: 时间戳+随机数
       6    mac: 用户Mac地址
       7    userid: 登陆用户id，未登录为0
       8    fpc: 策略、运营商和地域用户的地址，策略，isp信息
       9    version: 风行版本号
       10   sessionid: 当前会话超过30分钟生成新值
       11   pvid: 页面ID，每次刷新页面生成一个新值
       12   config: 页面唯一标识
       13   url: 当前url地址
       14   referurl: 当前burl来源url
       15   qudaoid: 渠道id
       16   vtime: 页面请求耗时
       17   pagetype: 扩展字段pagetype
       18   useragent: 用户操作系统、浏览器信息
       19   step: 用户史来play计数器，各自维护
       20   sestep: 本次session的play计数器，各自维护
       21   seidcount: 用户史来session计数器，各自维护
       22   ta: 用户策略
	 */
	PROTOCOL, RPROTOCOL, TIMESTAMP, IP,  CLIENTFLAG, FCK, MAC, USERID, FPC, VERSION, SESSIONID, 
	PVID, CONFIG, URL, REFERURL, QUDAO_ID, VTIME, PAGE_TYPE, USERAGENT, STEP, SESTEP, SEIDCOUNT,TA;
	//CLIENTFLAG,
}
