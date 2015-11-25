package com.bi.log.pv.format.dataenum;

public enum PvFormatEnum {
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
       16   vtime: 页面请求耗时
       17   pagetype: 扩展字段pagetype
       18   useragent: 用户操作系统、浏览器信息
       19   step: 用户史来play计数器，各自维护
       20   sestep: 本次session的play计数器，各自维护
       21   seidcount: 用户史来session计数器，各自维护
    
	New Field:
		日期, 小时, 版本号, 省份ID, 城市ID, MAC地址, 用户类别, 
		URL一级分类, URL一级分类, URL一级分类, 站内搜索关键词,第几页
		REFER一级分类,REFER一级分类, REFER一级分类, 站外搜索关键词,第几页
	 */
	
	
	DATE_ID, HOUR_ID, VERSION_ID, PROVINCE_ID, CITY_ID, MAC_CODE, USER_FLAG, 
	URL_FIRST_ID, URL_SECOND_ID, URL_THIRD_ID, INTER_SEARCH_KEYWORD, INTER_PAGE, 
	REFRE_FIRST_ID, REFER_SECOND_ID, REFER_THIRD_ID, OUTER_SEARCH_KEYWORD, OUTER_PAGE,
	BROWSRINFO,SYSTEMINFO,
	
	PROTOCOL, RPROTOCOL, TIMESTAMP, IP, FCK, MAC, USERID, FPC, VERSION, SESSIONID,
	PVID, CONFIG, URL, REFERURL, ISPID, VTIME, PAGE_TYPE, USERAGENT, STEP, SESTEP, SEIDCOUNT;
}