/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FormatMobileUtil.java 
 * @Package com.bi.mobile.comm.util 
 * @Description: 
 * @author fuys
 * @date 2013-5-7 下午1:32:56 
 */
package com.bi.mobile.comm.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bi.comm.util.MACFormatUtil;

/**
 * @ClassName: FormatMobileUtil
 * @Description:
 * @author fuys
 * @date 2013-5-7 下午1:32:56
 */
public class FormatMobileUtil {

    /**
     * 
     * 
     * @Title: FormatMobileUtil
     * @Description: 提取前两个版本信息
     * @param @param versionInfo
     * @param @return 参数说明
     * @return String 返回类型说明
     * @throws
     */
    public static String versionFormatMobileUtil(String versionInfo) {
        StringBuilder versionSB = new StringBuilder();
        String[] versions = versionInfo.split("\\.");
        if (null == versions || versions.length < 4) {

            versionSB.append("0.0");
        }
        else {

            versionSB.append(versions[0]);
            versionSB.append(".");
            versionSB.append(versions[1]);
        }
        return versionSB.toString();
    }

    /**
     * 
     * 
     * @Title: messageFormatMobileUtil
     * @Description: 处理消息类型(messagetype)
     * @param @param splitSts
     * @param @param enumClassStr
     * @param @return
     * @param @throws ClassNotFoundException 参数说明
     * @return String 返回类型说明
     * @throws
     */
    public static String messageFormatMobileUtil(String[] splitSts,
            String enumClassStr) throws Exception {

        Class<Enum> logEnum = (Class<Enum>) Class.forName(enumClassStr);
        String messageTypeStr = splitSts[Enum.valueOf(logEnum, "MESSAGETYPE")
                .ordinal()];
        return messageTypeStr;

    }

    /**
     * 
     * 
     * @Title: dealWithOk
     * @Description: 消息是否展示成功（ok）
     * @param @param splitSts
     * @param @param enumClassStr
     * @param @return
     * @param @throws Exception 参数说明
     * @return String 返回类型说明
     * @throws
     */

    public static String dealWithOk(String[] splitSts, String enumClassStr)
            throws Exception {

        Class<Enum> logEnum = (Class<Enum>) Class.forName(enumClassStr);
        String okStr = splitSts[Enum.valueOf(logEnum, "OK").ordinal()];
        return okStr;

    }

    /**
     * @throws Exception
     * @throws ClassNotFoundException
     * 
     * 
     * @Title: parseDoubleToLong
     * @Description: 小数取整
     * @param @param splitSts
     * @param @param enumClassStr
     * @param @return 参数说明
     * @return String 返回类型说明
     * @throws
     */
    public static long parseDoubleToLong(String[] splitSts,
            String enumClassStr, String keyCol) throws Exception {
        Class<Enum> logEnum = (Class<Enum>) Class.forName(enumClassStr);
        String dataStr = splitSts[Enum.valueOf(logEnum, keyCol).ordinal()];
        double dataDouble = Double.parseDouble(dataStr);
        long dataLong = filterByPositiveInteger((long) dataDouble);

        return dataLong;

    }

    /**
     * 
     * 
     * @Title: filterByPositiveInteger
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param tmplong
     * @param @return
     * @param @throws Exception 参数说明
     * @return long 返回类型说明
     * @throws
     */
    public static long filterByPositiveInteger(long tmplong) throws Exception {
        if (!(tmplong >= 0 && tmplong < 2147483647)) {

            throw new Exception("the value is not in positive integer!");
        }
        return tmplong;
    }

    public static void filerNoNumber(String ntTyepStr, String errorMegs)
            throws Exception {
        Pattern pattern = Pattern.compile("(-\\d)|(\\d)");
        Matcher matcher = pattern.matcher(ntTyepStr);
        if (!matcher.matches()) {

            throw new Exception("No Number format");
        }

    }

    public static String getMac(String[] fields, int platId, long versionId,
            String enumClassStr) throws Exception {
        Class<Enum> logEnum = (Class<Enum>) Class.forName(enumClassStr);
        boolean oldMac = platId == 5 || platId == 6 || platId == 7
                || platId == 8 || platId == 9
                || (platId == 3 && versionId < 16910594)
                || (platId == 4 && versionId < 16910082);
        String mac;
        if (oldMac) {
            mac = fields[Enum.valueOf(logEnum, "MAC").ordinal()];
            MACFormatUtil.isCorrectMac(mac);
            mac = MACFormatUtil.macFormatToCorrectStr(mac);
        }
        else {
            if (fields.length < Enum.valueOf(logEnum, "FUDID").ordinal() + 1) {
                throw new Exception("short ");
            }
            mac = fields[Enum.valueOf(logEnum, "FUDID").ordinal()];
        }
        return mac;
    }

}
