/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: CommonConstant.java 
 * @Package com.bi.common.constant 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-22 下午3:24:24 
 * @input:输入日志路径/2013-8-22
 * @output:输出日志路径/2013-8-22
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.constant;

/**
 * @ClassName: CommonConstant
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-22 下午3:24:24
 */
public interface CommonConstant {

    public static final String DM_MOBILE_PLATY = "dm_mobile_plat";

    public static final String DATE_FORMAT = "yyyyddmm";

    public static final String INPUT_PATH = "input";

    public static final String OUTPUT_PATH = "output";

    public static final String REDUCE_NUM = "reduceNum";

    public static final String EXECUTE_DATE = "executeDate";

    public static final String FORMAT_OUTPUT_DIR = "format/part";

    public static final String ERROR_OUTPUT_DIR = "error/part";

    public static final String ERROR_FORMAT_DIR = "_error/part";

    public static final String CL_FORMAT_OUTPUT_DIR = "_format_cl/part";

    public static final String IS_INPUTFORMATLZOCOMPRESS = "inpulzo";

    public static final String BOOTSTRAP = "bootstrap";

    public static final String DOWNLOAD = "download";

    public static final String FUBBER = "fbuffer";

    public static final String BTYPE = "btype";

    public static final long TOWHOUR = 2 * 60 * 60 * 1000;

    public static final long TOWMININUTES = 2 * 60 * 1000;

    public static final String TIMESPACE = "timespace";

    public static final String MIDFILENAME = "midFileName";

}
