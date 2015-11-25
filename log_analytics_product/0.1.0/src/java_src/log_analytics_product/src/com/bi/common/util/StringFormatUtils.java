/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: StringFormatUtils.java 
 * @Package com.bi.common.util 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-13 下午4:46:47 
 */
package com.bi.common.util;

/**
 * @ClassName: StringFormatUtils
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-13 下午4:46:47
 */
public class StringFormatUtils {
    public static final String DEFAULT_SEPARATOR = ",";

    public static String arrayToString(String[] strs, String separator) {
        if (null == separator || "".equalsIgnoreCase(separator)) {
            separator = DEFAULT_SEPARATOR;

        }

        if (strs.length == 0) {
            return "";
        }
        StringBuffer sbuf = new StringBuffer();
        sbuf.append(strs[0]);
        for (int idx = 1; idx < strs.length; idx++) {

            sbuf.append(separator);
            sbuf.append(strs[idx]);
        }
        return sbuf.toString();
    }

}
