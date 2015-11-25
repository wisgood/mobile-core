/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsplayAfterEnum.java 
 * @Package com.bi.common.logenum 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-11-26 上午11:13:06 
 * @input:输入日志路径/2013-11-26
 * @output:输出日志路径/2013-11-26
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.logenum;

/**
 * @ClassName: FsplayAfterEnum
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-11-26 上午11:13:06
 */
public enum FsplayAfterEnum {
    /**
     *  时间戳(timestamp): 用户ip(ip): 媒体ID(mid): 用户ID号(uid):为0，未登陆；
     * 设备mac地址(mac)：长度为16的大写字符串 用户唯一标识(fck)：长度为32的sha1字符串 任务infohash
     * id(ih)：长度为40的sha1字符串
     */
    TIMESTAMP, IP, MID, UID, MAC, FCK, IH;
}
