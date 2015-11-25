/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DataFormatUtils.java 
 * @Package com.bi.common.util 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-22 下午3:06:33 
 * @input:输入日志路径/2013-8-22
 * @output:输出日志路径/2013-8-22
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: DataFormatUtils
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-22 下午3:06:33
 */
public class DataFormatUtils {
    public static final char TAB_SEPARATOR = '\t';

    public static final char COMMA_SEPARATOR = ',';

    /**
     * 
     * 切分字符串
     * 
     * @param str
     *            被切分的字符串
     * 
     * @param separator
     *            分隔符字符
     * 
     * @param limit
     *            限制分片数
     * 
     * @return 切分后的集合
     */

    public static String[] split(String str, char separator, int limit) {
        if (0 == separator) {
            separator = COMMA_SEPARATOR;
        }
        String[] resultStrs = null;
        if (str == null) {
            return null;
        }
        List<String> list = new ArrayList<String>(limit == 0 ? 35 : limit);
        if (limit == 1) {
            list.add(str);
            resultStrs = new String[limit];
            return list.toArray(resultStrs);
        }
        boolean isNotEnd = true; // 未结束切分的标志
        int strLen = str.length();
        StringBuilder sb = new StringBuilder(strLen);
        for (int i = 0; i < strLen; i++) {
            char c = str.charAt(i);
            if (isNotEnd && c == separator) {
                list.add(sb.toString());
                // 清空StringBuilder
                sb.delete(0, sb.length());
                // 当达到切分上限-1的量时，将所剩字符全部作为最后一个串
                if (limit != 0 && list.size() == limit - 1) {
                    isNotEnd = false;
                }
            }
            else {
                sb.append(c);
            }
        }
        list.add(sb.toString());
        resultStrs = new String[list.size()];
        resultStrs = list.toArray(resultStrs);
        return resultStrs;
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

            throw new Exception(tmplong + " is not in positive integer!");
        }
        return tmplong;
    }

    public static void filerNoNumber(String ntTyepStr) throws Exception {
        Pattern pattern = Pattern.compile("(-\\d)|(\\d)");
        Matcher matcher = pattern.matcher(ntTyepStr);
        if (!matcher.matches()) {

            throw new Exception(ntTyepStr + " is not Number format");
        }

    }

    // 将十进制整数形式转换成127.0.0.1形式的ip地址
    public static String longToIP(long longIp) {
        StringBuffer sb = new StringBuffer("");
        // 直接右移24位
        sb.append(String.valueOf((longIp >>> 24)));
        sb.append(".");
        // 将高8位置0，然后右移16位
        sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));
        sb.append(".");
        // 将高16位置0，然后右移8位
        sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));
        sb.append(".");
        // 将高24位置0
        sb.append(String.valueOf((longIp & 0x000000FF)));
        return sb.toString();
    }

    public static String getDateIdFormPath(String filePath) {
        StringBuilder dateIdSB = new StringBuilder();
        int endIndex = filePath.length();
        String dateDirStr = new String(filePath.substring(endIndex - 10,
                endIndex));
        return dateDirStr.replaceAll("/", "");

    }

    public static void main(String[] args) {
        String[] s = { "16843777", "16844033", "16908545", "16908801",
                "16908802", "16909057", "16909058", "16909314", "16909570",
                "16909826", "16910082", "16910338", "16910593" };

        for (String temp : s)
            System.out.println(temp + "," + longToIP(Long.valueOf(temp)) + "");

    }
}
