package com.bi.comm.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StringUtils {

    public static String join(String[] array, String separator) {
        String retStr = null;

        int len = array.length;
        if (len >= 1) {
            retStr = array[0];
            for (int i = 1; i < len; i++) {
                retStr += separator + array[i];
            }
        }

        return retStr;

    }

    @SuppressWarnings("finally")
    public static String hex2dec(String col) {

        String retVal = null;
        String regex = ":|\\.";
        String newCol = col.replaceAll(regex, "");
        try {
            retVal = String.format("%s", Long.parseLong(newCol, 16));
        } finally {
            return retVal;
        }
    }

    @SuppressWarnings("finally")
    public static String ip2long(String col) {

        String retVal = "0";
        long[] nFields = new long[4];
        try {
            String[] strFields = col.split("\\.");
            for (int i = 0; i < 4; i++) {
                nFields[i] = Integer.parseInt(strFields[i]);
            }
            retVal = String.format("%s", (nFields[0] << 24)
                    | (nFields[1] << 16) | (nFields[2] << 8) | nFields[3]);
        } finally {
            return retVal;
        }
    }
/**
 * 
*
* @Title: StringToDate 
* @Description: 这里用一句话描述这个方法的作用 
* @param   @param dateStr
* @param   @param formatStr
* @param   @return 参数说明
* @return Date    返回类型说明 
* @throws
 */
    public static Date stringToDate(String dateStr, String formatStr) {
        DateFormat sdf = new SimpleDateFormat(formatStr);
        Date date = null;
        try {
            date = sdf.parse(dateStr);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        return date;
    }
}
