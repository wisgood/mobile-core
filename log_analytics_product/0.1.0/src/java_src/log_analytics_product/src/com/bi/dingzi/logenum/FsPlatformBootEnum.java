package com.bi.dingzi.logenum;

public enum FsPlatformBootEnum {
/**
 *   日志请求协议版本号，由前端发送，表明前端发送的版本号
                 启动类型：0-未知方式启动钉子，1-服务拉起钉子，2-计划任务拉起钉子，3-ie插件（BHO）拉起钉子，4-非ie插件（Firefox）拉起钉子（为以后功能预留），5-大安装包启动钉子，6-小安装包启动钉子，7- 双击启动钉子，8-ie插件（Active X）拉起钉子，9-NPAP插件拉起钉子
                客户端情况：0-没有客户端，1-有客户端没启动，2-有客户端启动
                同步工具状况：0-不存在，1-存在
                 本机mac地址
                计算机计算出来的用户标识，Globally Unique Identifier（全球唯一标识符）
                钉子名称：FSPAP（优），FSluncher（优）、Fsplatform（劣）、FsSvr（劣）
                 钉子版本
                  系统类型：other，xp，vista，win7-32，win7-64
 */
          
    
    PROTOCOL,RPROTOCOL,TIME,IP,BOOTMETHOD,CLIENTSTATE,SYNCTOOLSTATE,MAC,GUID,NAME,VERSION,OS;
    
}
