/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: NonnegativeNumber.java 
 * @Package com.bi.common.exception 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-30 下午5:05:41 
 * @input:输入日志路径/2013-8-30
 * @output:输出日志路径/2013-8-30
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.exception;

import com.bi.common.util.DataFormatUtils;

/**
 * @ClassName: NonnegativeNumber
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-30 下午5:05:41
 */
public class NonnegativeNumberException extends Exception {

    /** 
    * @Fields serialVersionUID : 用一句话描述这个变量表示什么 
    */ 
    private static final long serialVersionUID = 1033802136022001856L;
    private static final String EXCEPTION_MESSAGE = "is not in positive integer!";

    public NonnegativeNumberException(String orginLogStr, long errorValue) {
        // TODO Auto-generated constructor stub
        super(errorValue + EXCEPTION_MESSAGE + DataFormatUtils.TAB_SEPARATOR
                + orginLogStr);
    }
}
