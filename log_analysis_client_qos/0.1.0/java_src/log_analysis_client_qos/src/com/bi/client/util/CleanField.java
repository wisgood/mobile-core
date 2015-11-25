package com.bi.client.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CleanField
{
	public static class FieldValueMessyCodeException extends Exception
	{
		public FieldValueMessyCodeException(String fieldValue)
		{
			super("Some field with messy code: " + fieldValue);
		}
	}

	public static String cleanChineseFieldValue(String chineseFieldValue)
			throws FieldValueMessyCodeException
	{
		String cleanedValue = chineseFieldValue.trim().replaceAll("\\s{1,}", "");

		Pattern regexPattern;
		Matcher regexMatcher;

		// 1.如果是全英文的字段值，则直接返回
		regexPattern = Pattern.compile("^[a-z_A-Z]{1,}$");
		regexMatcher = regexPattern.matcher(cleanedValue);
		if (regexMatcher.matches())
		{
			return cleanedValue;
		}
		// 2.如果是全中文的字段值，则直接返回
		regexPattern = Pattern.compile("^[\u4E00-\u9FA5]{1,}$");
		regexMatcher = regexPattern.matcher(cleanedValue);
		if (regexMatcher.matches())
		{
			return cleanedValue;
		}
		// 3.如果包含其它非文字字符，则进行清洗，提取中文字段值并返回
		regexPattern = Pattern.compile(".*([\u4E00-\u9FA5]{1,}).*");
		regexMatcher = regexPattern.matcher(cleanedValue);
		if (regexMatcher.matches())
		{
			cleanedValue = cleanedValue.replaceAll("[^\u4E00-\u9FA5]", "");
			return cleanedValue;
		}

		// 4.如果是不规则的字符，则抛出异常
		throw new FieldValueMessyCodeException(chineseFieldValue);
	}

	public static String cleanMacFieldValue(String macFieldValue)
			throws FieldValueMessyCodeException
	{

		String cleanedValue = macFieldValue.trim().replaceAll("\\s{1,}", "");

		Pattern regexPattern = Pattern.compile("[a-fA-F0-9]{12}");
		Matcher regexMatcher = regexPattern.matcher(cleanedValue);

		if ("".equals(cleanedValue))
		{
			cleanedValue = "UNDEFINED";
		}
		else if (regexMatcher.matches())
		{
			cleanedValue = cleanedValue.toUpperCase();
		}
		else
		{
			throw new FieldValueMessyCodeException(macFieldValue);
		}

		return cleanedValue;
	}

	public static String cleanHexFieldValue(String hexFieldValue)
			throws FieldValueMessyCodeException
	{
		String cleanedValue = hexFieldValue.trim().replaceAll("\\s{1,}", "");

		Pattern regexPattern = Pattern.compile("NULL|null|[a-fA-F0-9]+");
		Matcher regexMatcher = regexPattern.matcher(cleanedValue);

		if ("".equals(cleanedValue))
		{
			cleanedValue = "UNDEFINED";
		}
		else if (regexMatcher.matches())
		{
			cleanedValue = cleanedValue.toUpperCase();
		}
		else
		{
			throw new FieldValueMessyCodeException(hexFieldValue);
		}

		return cleanedValue;
	}

}
