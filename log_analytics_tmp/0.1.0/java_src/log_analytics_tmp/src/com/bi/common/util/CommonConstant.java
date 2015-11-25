/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: CommonConstant.java 
* @Package com.bi.common.param 
* @Description: 对日志名进行处理
* @author fuys
* @date 2013-9-23 上午11:31:00 
* @input:输入日志路径/2013-9-23
* @output:输出日志路径/2013-9-23
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.common.util;

/** 
 * @ClassName: CommonConstant 
 * @Description: 这里用一句话描述这个类的作用 
 * @author fuys 
 * @date 2013-9-23 上午11:31:00  
 */
public interface CommonConstant {
    public static final String INPUT_PATH = "input";

    public static final String OUTPUT_PATH = "output";

    public static final String REDUCE_NUM = "reduceNum";

    public static final String EXECUTE_DATE = "executeDate";
    
    public static final String IS_INPUTFORMATLZOCOMPRESS = "inpulzo";
}
