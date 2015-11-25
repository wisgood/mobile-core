package com.bi.common.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bi.common.constant.ConstantEnum;


public class TimestampFormatUtil {
    private static Logger logger = Logger.getLogger(TimestampFormatUtil.class
            .getName());

    public static String formatTimeStamp(String timeStampStr) {
        SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd+HH:mm:ss");
        Date date = new Date(Long.valueOf(timeStampStr) * 1000);
        return time.format(date);
    }

    public static Map<ConstantEnum, String> formatTimestamp(String timeStampStr) {
        Map<ConstantEnum, String> formatTimesMap = new HashMap<ConstantEnum, String>();
        SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
        Date date = null;
        try {
            date = new Date(Long.valueOf(timeStampStr) * 1000);

        }
        catch(NumberFormatException e) {
            // TODO Auto-generated catch block
            // 获取当天凌晨时间
            date = get24HourMill();
            // e.printStackTrace();
            logger.error(e.getMessage(), e.getCause());
        }
        formatTimesMap.put(ConstantEnum.TIMESTAMP, timeStampStr);
        formatTimesMap.put(ConstantEnum.DATE_ID, time.format(date));
        formatTimesMap.put(ConstantEnum.HOUR_ID, date.getHours() + "");
        return formatTimesMap;
    }

    public static Map<ConstantEnum, String> formatRTTimeInfo(
            String rtTimeInfoStr) {

        Map<ConstantEnum, String> formatTimesMap = new HashMap<ConstantEnum, String>();
        SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
        Date date = null;
        try {
            date = new Date(Long.valueOf(rtTimeInfoStr));

        }
        catch(NumberFormatException e) {
            // TODO Auto-generated catch block
            // 获取当天凌晨时间
            date = get24HourMill();
            // e.printStackTrace();
            logger.error(e.getMessage(), e.getCause());
        }
        formatTimesMap.put(ConstantEnum.TIMESTAMP, rtTimeInfoStr);
        formatTimesMap.put(ConstantEnum.DATE_ID, time.format(date));
        formatTimesMap.put(ConstantEnum.HOUR_ID, date.getHours() + "");
        return formatTimesMap;
    }

    static Date get24HourMill() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.DAY_OF_MONTH, 0);
        long middleNightTimeLong = calendar.getTimeInMillis();
        Date date = new Date(middleNightTimeLong);
        return date;
    }
}
