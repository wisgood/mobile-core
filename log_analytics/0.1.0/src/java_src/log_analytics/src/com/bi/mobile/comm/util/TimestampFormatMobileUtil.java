package com.bi.mobile.comm.util;

import org.apache.log4j.Logger;

import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;


public class TimestampFormatMobileUtil {

	private static Logger logger = Logger.getLogger(TimestampFormatMobileUtil.class
			.getName());
	public static  java.util.Map<ConstantEnum, String> getFormatTimesMap(String orgdataStr,
			String[] splitSts, String enumClassTypeStr)
			throws ClassNotFoundException {
		Class<Enum> enumClass = (Class<Enum>) Class
				.forName(enumClassTypeStr);
		int networkType = Integer.parseInt(splitSts[Enum.valueOf(enumClass,
				ConstantEnum.NT.name()).ordinal()]);
		java.util.Map<ConstantEnum, String> formatTimesMap = null;
		int dateInforIndex = 0;
		if (-1 == networkType) {
			dateInforIndex = Enum.valueOf(enumClass,
					ConstantEnum.RT.name()).ordinal();
			String timeInfoStr = splitSts[dateInforIndex];
			 formatTimesMap = TimestampFormatUtil
						.formatRTTimeInfo(timeInfoStr);
			
		} else {
			dateInforIndex =Enum.valueOf(enumClass,
					ConstantEnum.TIMESTAMP.name()).ordinal();
			String timeInfoStr = splitSts[dateInforIndex];
			 formatTimesMap = TimestampFormatUtil
						.formatTimestamp(timeInfoStr);
		}
		String timeInfoStr = splitSts[dateInforIndex];
	    String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
		if(dateId.length() != "yyyymmdd".length()){
			logger.fatal("原始日志为:"+orgdataStr);
			logger.fatal("日期字段取的是:"+(dateInforIndex+1));
			logger.fatal("转换后的DateID:"+dateId);
		}
		return formatTimesMap;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
