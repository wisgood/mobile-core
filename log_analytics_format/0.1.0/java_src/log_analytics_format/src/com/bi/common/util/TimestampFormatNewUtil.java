/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: TimestampFormatNewUtil.java 
 * @Package com.bi.common.util 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-11-26 下午1:24:55 
 * @input:输入日志路径/2013-11-26
 * @output:输出日志路径/2013-11-26
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.bi.common.constant.UtilComstrantsEnum;

/**
 * @ClassName: TimestampFormatNewUtil
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-11-26 下午1:24:55
 */
public class TimestampFormatNewUtil {

    public static String formatTimestamp(String timeStampStr,
            String logRundateIdStr) {

        SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
        String resultValue = null;
        Date date = null;
        try {
            date = new Date(Long.valueOf(timeStampStr) * 1000);
            resultValue = time.format(date) + "\t" + date.getHours();
        }
        catch(NumberFormatException e) {
            // TODO Auto-generated catch block
            resultValue = logRundateIdStr + "\t"
                    + UtilComstrantsEnum.defaultHourId.getValueStr();
        }
        return resultValue;
    }

    public static String getTimestamp(String timeStampStr,
            String logRundateIdStr) {
        SimpleDateFormat timeDf = new SimpleDateFormat("yyyyMMdd");
        String resultValue = "0";
        try {
            new Date(Long.valueOf(timeStampStr) * 1000);
            resultValue = timeStampStr;
        }
        catch(NumberFormatException e) {
            // TODO Auto-generated catch block
            try {
                Date date = timeDf.parse(logRundateIdStr);
                resultValue = date.getTime() / 1000 + "";
            }
            catch(ParseException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        return resultValue;

    }

}
