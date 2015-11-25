package com.bi.ad.comm.util.format;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;

public class TimeFormat {

	private static Logger logger = Logger.getLogger(TimeFormat.class
			.getName());

	@SuppressWarnings("deprecation")
	public static HashMap<String, String> formatTimestamp(String timeStampStr) {
	    if(timeStampStr == null)
	        timeStampStr = "0";
		HashMap<String, String> formatTimesMap = new HashMap<String, String>();
		SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		Date date = null;
		try {
			date = new Date(Long.valueOf(timeStampStr) * 1000);

		} catch (NumberFormatException e) {
			//获取当日凌晨时间
			date = get24HourMill();
			//e.printStackTrace();
			logger.error(e.getMessage(), e.getCause());
		}
		formatTimesMap.put("TIMESTAMP", timeStampStr);
		formatTimesMap.put("DATE_ID", time.format(date));
		formatTimesMap.put("HOUR_ID", date.getHours() + "");
		return formatTimesMap;
	}
	
	public static Date get24HourMill() {
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
	
	public static String toString (String timeStampStr){
		HashMap<String, String> formatTime =  formatTimestamp(timeStampStr);
		return formatTime.get("TIMESTAMP") + "\t" + formatTime.get("DATE_ID") + "\t" + formatTime.get("HOUR_ID");
	}
	
}

