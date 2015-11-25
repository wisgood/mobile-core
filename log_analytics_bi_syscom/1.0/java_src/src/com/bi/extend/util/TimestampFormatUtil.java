package com.bi.extend.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.bi.common.constrants.UtilComstrantsEnum;

public class TimestampFormatUtil {

	public String formatTimestamp(String timeStampStr, String logRundateIdStr) {

		SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		String resultValue = null;
		Date date = null;
		try {
			date = new Date(Long.valueOf(timeStampStr) * 1000);
			resultValue = time.format(date) + "\t" + date.getHours();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			resultValue = logRundateIdStr + "\t"
					+ UtilComstrantsEnum.defaultHourId.getValueStr();
		}
		return resultValue;
	}

}
