package com.bi.ios.exchange.datadefine;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CustomEnumNameSet
{
	public static class CustomEnumNotFoundException extends Exception
	{
		public CustomEnumNotFoundException(String customEnumName)
		{
			super(customEnumName + "not found!");
		}

	}

	public static class CustomEnumFieldNotFoundException extends Exception
	{
		public CustomEnumFieldNotFoundException(String customEnumName, String customEnumFildName)
		{
			super(customEnumName + "'s field " + customEnumFildName + " not found!");
		}

	}

	//@formatter:off
	private static Set<String> customEnumNameSet = new HashSet<String>(
													Arrays.asList(
															new String[] 
															{
																"BootStrapEnum",
																"SpreadEnum",
																"TaskStatEnum"
															}));
	//@formatter:on

	public static boolean containsCustomEnumName(String customEnumName)
	{
		return customEnumNameSet.contains(customEnumName);
	}

	public static String foundCustomEnumName(String customEnumName)
			throws CustomEnumNotFoundException
	{
		if (customEnumNameSet.contains(customEnumName))
		{
			return customEnumName;
		}
		throw new CustomEnumNotFoundException(customEnumName);
	}

	public static int getCustomEnumFieldOrder(String customEnumName, String customEnumFieldName)
			throws CustomEnumFieldNotFoundException
	{
		int customFieldOrder = -1;

		if ("BootStrapEnum".equals(customEnumName))
		{
			customFieldOrder = BootStrapEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("SpreadEnum".equals(customEnumName))
		{
			customFieldOrder = SpreadEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("TaskStatEnum".equals(customEnumName))
		{
			customFieldOrder = TaskStatEnum.getFieldOrder(customEnumFieldName);
		}

		return customFieldOrder;
	}

}
