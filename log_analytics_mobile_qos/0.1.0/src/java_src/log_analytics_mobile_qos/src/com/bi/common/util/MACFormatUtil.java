package com.bi.common.util;

/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: MACFormatUtil.java 
 * @Package com.bi.common.util 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-22 下午3:40:39 
 * @input:输入日志路径/2013-8-22
 * @output:输出日志路径/2013-8-22
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */


import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: MACFormatUtil
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-22 下午3:40:39
 */
public class MACFormatUtil {

    /**
     * 
     * @param macStr
     * @return
     * @throws Exception
     */
    public static void isCorrectMac(String macStr) throws Exception {
        String macRex = "[0-9a-fA-F]{32}|"
                + "([0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2})|"
                + "[0-9a-fA-F]{12}|"
                + "[0-9a-fA-F]{8}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{12}"
                + "|null|\\(null\\)|NULL|\\(NULL\\)";
        Pattern pattern = Pattern.compile(macRex);
        Matcher matcher = pattern.matcher(macStr);
        if (!matcher.matches() && !macStr.equalsIgnoreCase("")) {
            throw new Exception("MAC address errors");
        }
    }

    public static String macFormatToCorrectStr(String macStr) {
        String returnMacStr = macStr;
        if (macStr.equals(""))
            return "NULL";
        if (macStr.contains(":")) {
            returnMacStr = macStr.replaceAll(":", "");
        }
        if (macStr.contains(".")) {
            returnMacStr = macStr.replaceAll("\\.", "");
        }
        return returnMacStr.toUpperCase();

    }

}
