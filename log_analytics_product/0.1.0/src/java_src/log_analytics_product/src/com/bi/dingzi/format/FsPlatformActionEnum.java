package com.bi.dingzi.format;

public enum FsPlatformActionEnum {
/**
 * 
 * 
 * 
 * "rprotocol",         # 日志请求协议版本号，由前端发送，表明前端发送的版本号
    "action",            # 钉子动作，以字符串形式显示：PullupClient-钉子拉客户端（包括正式版和绿色版），PullupPushTool-钉子拉同步工具，DownloadGclient-钉子下载绿色版客户端，ScreenSaverPullClient-屏保触发钉子拉客户端，ScreenSaverPullDloader-屏保触发钉子拉下载器，ClockScreenPullupClient-锁屏触发钉子拉客户端，ClockScreenPullDloader-锁屏触发钉子拉下载器，ScreenSaver-用户机器进入屏保钉子上报，LockScreen-用户机器进入锁屏钉子上报
    "actionresult",      # 钉子动作行为结果，1X表示拉客户端结果（10：拉起失败，11：拉起成功，12：本地客户端已经启动不拉，13：LastBootedTime与本地时间同一天不拉），2X表示拉起同步工具（20：android拉起失败，21：android拉起成功，22：ios拉起失败，23：ios拉起成功），3X表示下载绿色版客户端（30：下载失败，31：下载成功），4X表示屏保触发钉子拉客户端（40：拉起失败，41：拉起成功，42：策略不拉，43：已有不拉，44：屏保退出不拉），5X表示屏保触发钉子拉下载器（50：下载失败，51：下载成功，52：本地调用，53：策略不下载），6X表示锁屏触发钉子拉客户端（60：拉起失败，61：拉起成功，62：策略不拉，63：已有不拉，64：锁屏退出不拉），7X表示锁屏触发钉子拉下载器（70：下载失败，71：下载成功，72：本地调用，73：策略不下载），用户机器进入屏保和锁屏时该字段为空
    "actionobjectver",   # 钉子交互对象版本，字符串，10-拉起客户端失败时报，client+version（客户端版本）-拉起客户端成功时，20-拉起同步工具失败时报，pushtool+version（同步工具版本）-拉起同步工具成功时，screendll+veraion（屏保或锁屏相应的dll版本）-屏保/锁屏时触发钉子拉客户端或下载器，其余情况此自段为空
    "channelid",         # 渠道id，当拉起客户端时：00-拉起正式版客户端失败及无客户端，normal+channelid-拉起正式版客户端成功，10-拉起绿色版客户端失败及无客户端，green+channelid-拉起绿色版客户端成功；其余情况该字段为空 
    "mac",               # 本机mac地址
    "guid",              # 计算机计算出来的用户标识，Globally Unique Identifier（全球唯一标识符）
    "name",              # 钉子名称：FSPAP（优），FSluncher（优）、Fsplatform（劣）、FsSvr（劣）
    "version",           # 钉子版本
    "actiontime",        # 零点操作时机：只有在拉起客户端和下载绿色版客户端时且只在00:10分和00：30有动作时上报动作的时间点：00:10或00:30，其他情况报null
 * 
 * 
 * 
 */
    
    PROTOCOL,RPROTOCOL,TIME,IP, ACTION, ACTIONRESULT, ACTIONOBJECTVER, CHANNELID, MAC, GUID,NAME, VERSION, ACTIONTIME;
}
