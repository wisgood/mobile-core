package com.bi.website.mediaplay.tmp.muvs;

public enum PVDataEnum {

    /**
     * 上报项目
    time:时间戳
    ip：IP地址
    mac：用户的Mac信息
    useid:（用户登录ID）
    fpc:策略、运营商和地域用户的地址，策略，isp信息
    url：当前页面地址
    referrer：来源页面
    block：从哪一区块点击而来
    type:点击(click)，展现(view)
    target：媒体id_种子id（ 和点击相关的target请求页面）
    vtime：当前请求时间与页面加载开始时间的差值(毫秒){页面展现耗时 (从浏览器收到html代码开始到页面展现完成用时 ms，只对pv的页面有效)}
    hz：用户使用风行频率
    channel：用户所属渠道信息 (主要用于新用户首页)
    version：风行版本号
    setup：“1”表示已安装风行软件
    cindex:点击位置
    track:来源区块
    fck:用户唯一标识{排重参数}
     */
    TIMESTAMP, IP,MAC,USERID,FPC,URL,REFERRER,BLOCK,TYPE,TARGET,VTIME,HZ,CHANNEL,VERSION,SETUP,CINDEX,TRACK,FCK;
}
