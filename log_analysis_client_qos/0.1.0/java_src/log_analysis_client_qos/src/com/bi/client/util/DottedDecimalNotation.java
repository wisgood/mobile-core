package com.bi.client.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DottedDecimalNotation
{
	public static class DottedDecimalNotationException extends Exception
	{
		public DottedDecimalNotationException(String msg)
		{
			super(msg);
		}
	}

	private static boolean check(String dotDecStr)
	{
		Pattern pattern = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
		Matcher matcher = pattern.matcher(dotDecStr);
		if (matcher.matches())
		{
			return true;
		}
		return false;
	}

	public static String format(String dotDecStr) throws DottedDecimalNotationException
	{
		if (("".equals(dotDecStr.trim())) || null == dotDecStr)
		{
			return "0.0.0.0";
		}
		else if (check(dotDecStr.trim()))
		{
			return dotDecStr.trim();
		}
		else
		{
			throw new DottedDecimalNotationException("DotDecNum is illegal: " + dotDecStr);
		}
	}

	public static long dotDec2Dec(String dotDecStr) throws DottedDecimalNotationException
	{
		String decNum = "0";
		long[] decFields = new long[4];
		dotDecStr = format(dotDecStr);
		String[] strFields = dotDecStr.split("\\.");
		for (int i = 0; i < 4; i++)
		{
			decFields[i] = Integer.parseInt(strFields[i]);
		}
		decNum = String.format("%s", (decFields[0] << 24) | (decFields[1] << 16)
				| (decFields[2] << 8) | decFields[3]);

		return Long.parseLong(decNum);
	}

	public static String dec2DotDec(long dec) throws DottedDecimalNotationException
	{
		if (0 <= dec)
		{
			String dotDec;

			long[] parts = new long[4];

			parts[0] = dec >>> 24;

			parts[1] = (dec << 40) >>> 56;

			parts[2] = (dec << 48) >>> 56;

			parts[3] = (dec << 56) >>> 56;

			dotDec = parts[0] + "." + parts[1] + "." + parts[2] + "." + parts[3];

			return dotDec;
		}
		else
		{
			throw new DottedDecimalNotationException("DecNum is negative: " + dec);
		}
	}

}
