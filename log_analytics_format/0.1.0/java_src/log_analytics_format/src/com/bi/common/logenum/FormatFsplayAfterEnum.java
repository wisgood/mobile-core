/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FormatFsplayAfterEnum.java 
 * @Package com.bi.common.logenum 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-11-26 上午11:13:44 
 * @input:输入日志路径/2013-11-26
 * @output:输出日志路径/2013-11-26
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.logenum;

/**
 * @ClassName: FormatFsplayAfterEnum
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-11-26 上午11:13:44
 */
public enum FormatFsplayAfterEnum {
    DATE_ID, HOUR_ID, PROVINCE_ID, CITY_ID, ISP_ID, PLAT_ID, INFOHASH_ID, SERIAL_ID, MEDIA_ID, CHANNEL_ID, MEDIA_NAME, MAC, FCK, UID, TIME_STAMP;
}
