/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DateFormat.java 
 * @Package com.bi.common.util 
 * @Description: ����־����д���
 * @author fuys
 * @date 2013-7-20 ����4:37:36 
 * @input:������־·��/2013-7-20
 * @output:�����־·��/2013-7-20
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: DateFormat
 * @Description: ������һ�仰��������������
 * @author fuys
 * @date 2013-7-20 ����4:37:36
 */
public class DateFormat {
    /**
     * 
     * 
     * @Title: getDateIDStrFormFilePath
     * @Description: ��·���л�ȡ��־ʱ��
     * @param @param filePath
     * @param @return ����˵��
     * @return String ��������˵��
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

    public static String getAppType(String prefixStr, String appTypeStr) {

        StringBuilder dateIdSB = new StringBuilder();
        String regEx = prefixStr + "(\\d+)";

        Pattern pat = Pattern.compile(regEx);
        if (pat.matcher(appTypeStr).matches()) {

            return appTypeStr;
        }
        return null;

    }

    public static List<String> splitSimpleString(String source, char gap) {
        List<String> result = new LinkedList<String>();
        char[] sourceChars = source.toCharArray();
        String section = null;
        int startIndex = 0;
        for (int index = -1; ++index != sourceChars.length;) {
            if (sourceChars[index] != gap)
                continue;
            section = source.substring(startIndex, index);
            result.add(section);
            startIndex = index + 1;
        }
        section = source.substring(startIndex, sourceChars.length);
        result.add(section);
        return result;
    }

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

}
