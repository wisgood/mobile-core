package com.bi.common.etl.transform;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

public class TimestampTransform implements Transform {
    private static Logger logger = Logger.getLogger(TimestampTransform.class
            .getName());
    
    private Date date = null;
    private SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
    
    public TimestampTransform(){
//        System.out.println("TimestampTransform---------------------------");
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

    @Override
    public String process(String origin, String sperator) {
        String result;
        try {
            date = new Date(Long.valueOf(origin) * 1000);

        }
        catch(NumberFormatException e) {
            // 获取当天凌晨时间
            date = get24HourMill();
            // e.printStackTrace();
            logger.error(e.getMessage(), e.getCause());
        }
        result = time.format(date);
        result += sperator + date.getHours();
        
        return result;
    }
}
