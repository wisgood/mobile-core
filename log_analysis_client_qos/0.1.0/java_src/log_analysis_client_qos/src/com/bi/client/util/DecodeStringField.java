package com.bi.client.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class DecodeStringField
{
	public static String decode(String srcStringField, String charsetName)
			throws UnsupportedEncodingException
	{
		String decodedStringField = null;
		String changedStringField = DecodeStringField.changeCharset(srcStringField, charsetName);
		try
		{
			decodedStringField = URLDecoder.decode(URLDecoder.decode(
					URLDecoder.decode(changedStringField, charsetName), charsetName), charsetName);
		}
		catch (IllegalArgumentException e)
		{
			decodedStringField = "UNKNOWN";
		}

		return decodedStringField;
	}

	private static String changeCharset(String srcStringField, String charsetName)
			throws UnsupportedEncodingException
	{
		String destStringField = null;
		if (("".equals(srcStringField.trim())) || (null == srcStringField))
		{
			destStringField = new String("UNDEFINED".getBytes(), charsetName);
		}
		else
		{
			destStringField = new String(srcStringField.getBytes(), charsetName);
		}
		return destStringField;
	}

}
