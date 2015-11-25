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
 * @ClassName: PlayerSEOFormatEnum
 * @Description: player_seo log Format fields descripion
 * @author liuyn
 * @date Aug 28, 2013 2:19:20 AM
 */

public enum PlayerSEOFormatEnum {
    /**
     * Timestamp,
     * Ip,
     * type, show=展现、close=关闭、down=下载 
     * where, s=底栏提醒、m=悬浮提醒, n=表示北上策略底栏提醒
     * fck, 
     * mac, 
     * clifz, 合作商 
     * qudao: 渠道号
     * 
     */
    DATE_ID, HOUR_ID, PROVINCE_ID, CITY_ID, MAC_CODE, COOPTYPE_ID, SHOWLOC_ID, 
    TIMESTAMP, IP, TYPE, WHERE, FCK, MAC, CLIFZ, QUDAO_ID
}
