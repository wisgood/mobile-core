/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DateFormat.java 
 * @Package com.bi.comm.util 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-20 下午2:32:15 
 */
package com.bi.comm.util;

import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: DateFormat
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-20 下午2:32:15
 */
public class DateFormat {

    /**
     * 
     * 
     * @Title: getDateIDStrFormFilePath
     * @Description: 从路基中获取日志时间
     * @param @param filePath
     * @param @return 参数说明
     * @return String 返回类型说明
     * @throws
     */
    public static String getDateIDStrFormFilePath(String filePath) {
        StringBuilder dateIdSB = new StringBuilder();
        String regEx = "(\\d+\\/\\d+\\/\\d+)";

        Pattern pat = Pattern.compile(regEx);
        Matcher mat = pat.matcher(filePath);
        while (mat.find()) {
            for (int i = 1; i <= mat.groupCount(); i++) {
                dateIdSB.append(mat.group(i));
            }
        }
        return dateIdSB.toString().replaceAll("/", "");
    }

    public static String getDateIdFormPath(String filePath) {
        StringBuilder dateIdSB = new StringBuilder();
        int endIndex = filePath.length();
        String dateDirStr = new String(filePath.substring(endIndex - 10,
                endIndex));
        return dateDirStr.replaceAll("/", "");

    }
    
    public static String decodeURL(String urlStr) throws Exception {
        String str = URLDecoder.decode(urlStr, "iso-8859-1");
        String rule = "^(?:[\\x00-\\x7f]|[\\xe0-\\xef][\\x80-\\xbf]{2})+$";
        if (str.matches(rule)) {
            return URLDecoder.decode(urlStr, "UTF-8");
        }
        else {
            return URLDecoder.decode(urlStr, "GBK");
        }
    }

}
