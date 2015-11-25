/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: LengthLessThanRequiredException.java 
 * @Package com.bi.common.exception 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-30 下午4:44:02 
 * @input:输入日志路径/2013-8-30
 * @output:输出日志路径/2013-8-30
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.exception;

import com.bi.common.util.DataFormatUtils;

/**
 * @ClassName: LengthLessThanRequiredException
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-30 下午4:44:02
 */
public class LengthLessThanRequiredException extends Exception {

    /**
     * @Fields serialVersionUID : 用一句话描述这个变量表示什么
     */
    private static final long serialVersionUID = 3774903453715891679L;

    private static final String EXCEPTION_MESSAGE = "The length of this log less than";

    public LengthLessThanRequiredException(String orginLogStr,
            int requiredLength) {
        // TODO Auto-generated constructor stub
        super(EXCEPTION_MESSAGE + requiredLength + "."
                + DataFormatUtils.TAB_SEPARATOR + orginLogStr);
    }

}
