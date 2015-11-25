package com.bi.ibidian.datadefine;

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
																"HotsSpeedEnum", 
																"IbidianClickEnum", 
																"IbidianPVEnum", 
																"IbidianSpeedEnum" 
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

		if ("HotsSpeedEnum".equals(customEnumName))
		{
			customFieldOrder = HotsSpeedEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("IbidianClickEnum".equals(customEnumName))
		{
			customFieldOrder = IbidianClickEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("IbidianPVEnum".equals(customEnumName))
		{
			customFieldOrder = IbidianPVEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("IbidianSpeedEnum".equals(customEnumName))
		{
			customFieldOrder = IbidianSpeedEnum.getFieldOrder(customEnumFieldName);
		}
		return customFieldOrder;
	}
}
