package com.bi.client.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class PNCalculation
{

	public static class PNCalculationException extends Exception
	{
		public PNCalculationException(String msg)
		{
			super(msg);
		}

	}

	public static String pnCalculate(Map<Long, Long> indsFrequenceMap, final int indsNum,
			final long recordNum) throws PNCalculationException
	{
		if (indsNum == indsFrequenceMap.keySet().size())
		{
			long[] indsArray = new long[indsNum];

			Iterator<Long> indsSetItor = indsFrequenceMap.keySet().iterator();

			int arrIndex = 0;

			while (indsSetItor.hasNext())
			{
				long indicator = indsSetItor.next();

				indsArray[arrIndex] = indicator;

				arrIndex++;
			}

			Arrays.sort(indsArray);

			StringBuilder pnBuilder = new StringBuilder();

			double indsFrequenceSum = 0;
			int pnIndex = 1;
			for (int i = 0; i < indsNum; i++)
			{
				long indicator = indsArray[i];
				long frequence = indsFrequenceMap.get(indicator);
				indsFrequenceSum += frequence;

				int proportion = (int) ((indsFrequenceSum / recordNum) * 100);

				while (pnIndex <= proportion)
				{
					pnBuilder.append(indicator);
					pnBuilder.append(",");

					pnIndex++;
				}
			}

			String pnStr = pnBuilder.toString();
			return pnStr.substring(0, pnStr.length() - 1);
		}
		else
		{
			throw new PNCalculationException(
					"PN-Map's keys quantity is differ from the parameter value!");
		}
	}

	public static String pnIndicators(Map<Long, Long> indsFrequenceMap, final int indsNum,
			final long recordNum) throws PNCalculationException
	{
		if (indsNum == indsFrequenceMap.keySet().size())
		{
			long[] indsArray = new long[indsNum];

			Iterator<Long> indsSetItor = indsFrequenceMap.keySet().iterator();

			int arrIndex = 0;

			while (indsSetItor.hasNext())
			{
				long indicator = indsSetItor.next();

				indsArray[arrIndex] = indicator;

				arrIndex++;
			}

			Arrays.sort(indsArray);

			StringBuilder indsBuilder = new StringBuilder();

			for (int i = 0; i < indsNum; i++)
			{
				long indicator = indsArray[i];
				long frequence = indsFrequenceMap.get(indicator);

				indsBuilder.append(indicator);
				indsBuilder.append(":");
				indsBuilder.append(frequence);
				indsBuilder.append(",");
			}

			String indsStr = indsBuilder.toString();
			return indsStr.substring(0, indsStr.length() - 1);
		}
		else
		{
			throw new PNCalculationException(
					"PN-Map's keys quantity is differ from the parameter value!");
		}
	}

}
