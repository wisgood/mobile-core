/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: MACaddressErrorExcetion.java 
 * @Package com.bi.common.exception 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-30 下午4:56:42 
 * @input:输入日志路径/2013-8-30
 * @output:输出日志路径/2013-8-30
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.exception;

import com.bi.common.util.DataFormatUtils;

/**
 * @ClassName: MACaddressErrorExcetion
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-30 下午4:56:42
 */
public class MACaddressErrorExcetion extends Exception {

    /**
     * @Fields serialVersionUID : 用一句话描述这个变量表示什么
     */
    private static final long serialVersionUID = 9014741946066661439L;

    private static final String EXCEPTION_MESSAGE = "MAC address errors";

    public MACaddressErrorExcetion(String orginLogStr, String errorMacStr) {
        // TODO Auto-generated constructor stub
        super(EXCEPTION_MESSAGE + errorMacStr + "."
                + DataFormatUtils.TAB_SEPARATOR + orginLogStr);
    }
}
