/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayerSEOEnum.java 
 * @Package com.bi.log.player_seo.format.dataenum 
 * @Description: 对日志名进行处理
 * @author hadoop
 * @date Aug 28, 2013 2:19:20 AM 
 * @input:输入日志路径/Aug 28, 2013
 * @output:输出日志路径/Aug 28, 2013
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.log.playerSEO.format.dataenum;

/**
 * @ClassName: PlayerSEOEnum
 * @Description: player_seo log Format fields descripion
 * @author liuyn
 * @date Aug 28, 2013 2:19:20 AM
 */

public enum PlayerSEOEnum {
    /**
     * Timestamp,
     * Ip,
     * cooptype, show=展现、close=关闭、down=下载 
     * where, s=底栏提醒、m=悬浮提醒, n=表示北上策略底栏提醒, remindBox where
     * fck, 
     * mac, 
     * clifz, 合作商 
     * Qudao: 渠道号
     * 
     */
    
    TIMESTAMP, IP, COOPTYPE, WHERE, FCK, MAC, CLIFZ, QUDAO_ID
}
