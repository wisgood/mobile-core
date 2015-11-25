package com.bi.ibidian.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TimeStamp
{
	public static long getSecTimeStamp(String timeStr) throws NumberFormatException
	{
		long secsTS = milliedTimeStamp(timeStr.trim()) / 1000;
		return secsTS;
	}

	public static long getMilliTimeStamp(String timeStr) throws NumberFormatException
	{
		long milliTS = milliedTimeStamp(timeStr.trim());
		return milliTS;
	}

	public static int getDateID(String timeStr) throws NumberFormatException
	{
		long milliTS = milliedTimeStamp(timeStr.trim());
		Date date = new Date(milliTS);
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		int dateID = Integer.parseInt(dateFormat.format(date));
		return dateID;
	}

	public static int getHourID(String timeStr) throws NumberFormatException
	{
		long milliTS = milliedTimeStamp(timeStr.trim());
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(milliTS);
		int hourID = calendar.get(Calendar.HOUR_OF_DAY);
		return hourID;
	}

	private static long milliedTimeStamp(String timeStr)
	{
		long milliTS = 0;
		if (10 == timeStr.length())
		{
			try
			{
				milliTS = Long.parseLong(timeStr) * 1000;
			}
			catch (NumberFormatException e)
			{
				throw new NumberFormatException("TimeStampStr is illegal: " + timeStr);
			}
		}
		else if (13 == timeStr.length())
		{
			try
			{
				milliTS = Long.parseLong(timeStr);
			}
			catch (NumberFormatException e)
			{
				throw new NumberFormatException("TimeStampStr is illegal: " + timeStr);
			}
		}
		else
		{
			Calendar rightNow = Calendar.getInstance();
			GregorianCalendar ggc = new GregorianCalendar(rightNow.get(Calendar.YEAR),
					rightNow.get(Calendar.MONTH), rightNow.get(Calendar.DAY_OF_MONTH));
			ggc.add(Calendar.DATE, -1);
			Date date = ggc.getTime();
			milliTS = date.getTime();
		}
		return milliTS;
	}

}
