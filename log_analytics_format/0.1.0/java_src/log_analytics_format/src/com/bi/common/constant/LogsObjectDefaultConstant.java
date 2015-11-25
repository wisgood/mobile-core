/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: LogsObjectDefaultConstant.java 
* @Package com.bi.common.constant 
* @Description: 对日志名进行处理
* @author fuys
* @date 2013-8-30 上午11:01:23 
* @input:输入日志路径/2013-8-30
* @output:输出日志路径/2013-8-30
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.common.constant;

/** 
 * @ClassName: LogsObjectDefaultConstant 
 * @Description: 这里用一句话描述这个类的作用 
 * @author fuys 
 * @date 2013-8-30 上午11:01:23  
 */
public interface LogsObjectDefaultConstant {

    
    //DATE_ID, HOUR_ID, PLAT_ID, VERSION_ID, PROVINCE_ID, ISP_ID, SERVER_ID, NET_TYPE
    public static final int DATE_ID_DEFAULT_VALUE = 20130820;
    public static final int HOUR_ID_DEFAULT_VALUE= 20;
    public static final int  PLAT_ID_DEFAULT_VALUE = 11;
    public static final long  VERSION_ID_DEFAULT_VALUE = -1;
    public static final int PROVINCE_ID_DEFAULT_VALUE = -1;
    public static final int ISP_ID_DEFAULT_VALUE =-1;
    public static final int SERVER_ID_DEFAULT_VALUE =-1;
    public static final int NET_TYPE_DEFAULT_VALUE =-1;
    
    public static final String MAC_DEFAULT_VALUE ="NULL";
    public static final String IP_DEFAULT_VALUE ="0.0.0.0";
    public static final int OK_DEFAULT_VALUE = -9999;
    public static final long BPOS_DEFAULT_VALUE=0; 
    public static final long BTM_DEFAULT_VALUE=0; 
    public static final long DRATE_DEFAULT_VALUE=0; 
}
