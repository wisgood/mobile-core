/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: PVNewEnum.java 
* @Package com.bi.website.tmp.pv.bouncerate.caculate 
* @Description: 用一句话描述该文件做什么
* @author fuys
* @date 2013-5-23 上午11:24:04 
*/
package com.bi.website.tmp.pv.bouncerate.caculate;

/** 
 * @ClassName: PVNewEnum 
 * @Description: 这里用一句话描述这个类的作用 
 * @author fuys 
 * @date 2013-5-23 上午11:24:04  
 */
public enum PVNewEnum {

    /**
     * 
     * #—————基本设置——————————————————————————————————————————————————#
#采集组根据该协议规定的字段给到数据组

# 一级目录名称
firstname=website

# 二级目录名称
# 必填项，字符串类型，不区分大小写，只包含26个英文字母
secondname=pv

# 协议的类型
# 必填项，字符串类型，不区分大小写，可选值为：request(请求协议)、logfile(日志协议)、convert(转换协议)
type=logfile

# 协议的版本
# 必填项，整形，大于零的整数
version=1

# 协议是否激活
# 可选项，布尔类型，不区分大小写，可选值为：true(激活)、false(不激活日志协议)。若未设置，默认值为true。
active=true

# 协议的主体内容中，用于分隔多个参数的分隔符
# 可选项，字符串类型，区分大小写，特殊字符。若未设置，默认值为：\t
split=\t


# 协议的上线时间
# 可选项，格式为yyyy-MM-dd。若未设置，则表示上线时间未知
activeDate=2013-04-01

#—————最终输出的日志相关的设置—————————————————————————————————————————————#

# 最终输出的日志格式为
    #1. 纯文本，每行一条记录
    #2. 每行都是tab键作为分隔符，共有25个位置(P0到P24)，形如："P0    P1  P2  P3  P4  P5  P6  P7  P8  P9  P10 P11 ..."

#—————参数说明———————————————————————————————————————————————#

# 参数的数量
# 必填项，整形，大于零的整数
size=21

# 格式：Pn=参数名:参数类型:参数描述
#       n为自然数序列，从0开始，按顺序排列直到size-1
#       参数名：必填项，字符串类型，区分大小写
#       参数类型：必填项，字符串类型，不区分大小写，目前支持：int(整形)、long(长整形)、string(字符串)
#       参数描述：可选项，字符串类型

P0=protocol:int:该字段为该协议的版本号，每个协议不管任何一部分作变动，该版本号都要升级跟踪
P1=rprotocol:int:该字段为pv-request协议的版本号
P2=time:long:日志记录时间
P3=ip:string:用户ip
P4=fck:string:由js代码加入cookie的唯一标识，用以标识唯一用户
P5=mac:string:mac地址，安装风行客户端的机器来获取
P6=userid:int:登录用户注册id，如果未登录为0
P7=fpc:string:策略、运营商和地域用户的地址，策略，isp信息
P8=version:int:风行版本号
P9=sid:string:当前会话ID，由js生成，算法跟fck类似，生命周期定义为30分钟
P10=pvid:string:页面ID，每次刷新页面生成一个新值（UUID算法）
P11=config:string:页面唯一标示，页面分类
p12=url:string:当前url地址
p13=referurl:string:前链url
p14=channelid:string:合作渠道id
p15=vtime:long:页面请求耗时
p16=ext:string:扩展字段pagetype=?&（key=value）
p17=step:int:格式：用户史来pv计数器，各自维护
p18=sestep:string:格式：本次session的pv计数器，各自维护
p19=seidcount:string:用户史来session计数器，各自维护
p20=useragent:string:用户的操作系统、浏览器信息
     * 
     * 
     */
    PROTOCOL,REPROTOCOL,TIMESTAMP,IP,FCK,MAC,USERID,FPC,VER,SESSIONID,PVID,CONFIG,URL,REFERURL,CHANNELID,VTIME,EXT,STEP,SESTEP,SEIDCOUNT,USERAGENT;
}
