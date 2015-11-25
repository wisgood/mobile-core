/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: StringDecodeFormatUtil.java 
 * @Package com.bi.comm.util 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-22 下午1:50:29 
 */
package com.bi.comm.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * @ClassName: StringDecodeFormatUtil
 * @Description: 这里用一句话描述这个类的作用
 * @author wanghh
 * @date 2013-5-22 下午1:50:29
 */
public class StringDecodeFormatUtil {
    /**
     * 
     * 
     * @Title: changeCharset
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param str
     * @param @param targetCharset
     * @param @return
     * @param @throws UnsupportedEncodingException 参数说明
     * @return String 返回类型说明
     * @throws
     */
    public static String changeCharset(String str, String targetCharset)
            throws UnsupportedEncodingException {
        String targetStr = null;
        if (str != null) {
            byte[] byteString = str.getBytes();
            targetStr = new String(byteString, targetCharset);
        }
        return targetStr;
    }

    /**
     * 
     * 
     * @Title: decodeCodedStr
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param sourceStr
     * @param @param targetCharset
     * @param @return
     * @param @throws UnsupportedEncodingException 参数说明
     * @return String 返回类型说明
     * @throws
     */
    public static String decodeCodedStr(String sourceStr, String targetCharset)
            throws UnsupportedEncodingException {
        String decodedStr;
        String changedStr = changeCharset(sourceStr, targetCharset);
        if (changedStr != null) {
            try {
                decodedStr = URLDecoder.decode(
                        URLDecoder.decode(changedStr, targetCharset),
                        targetCharset);
            }
            catch(Exception e) {
                decodedStr = "unknown";
            }
            return decodedStr;
        }
        return null;
    }
}
