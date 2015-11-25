package com.bi.client.datadefine;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.login.LoginException;

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
																"PlayErrorEnum",
																"FSPlayAfterEnum",
																"BootEnum",
																"DumpEnum",
																"PlayFailReportEnum",
																"DtfspEnum",
																"DtjsEnum",
																"InlinePageEnum",
																"PlayBufferingEnum",
																"PlayerBuffEnum",
																"PlayHaltDetailEnum",
																"PlayHaltEnum",
																"TaskfluxSourceEnum",
																"HSEnum",
																"LoginEnum"
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

		if ("PlayErrorEnum".equals(customEnumName))
		{
			customFieldOrder = PlayErrorEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("FSPlayAfterEnum".equals(customEnumName))
		{
			customFieldOrder = FSPlayAfterEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("BootEnum".equals(customEnumName))
		{
			customFieldOrder = BootEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("DumpEnum".equals(customEnumName))
		{
			customFieldOrder = DumpEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("PlayFailReportEnum".equals(customEnumName))
		{
			customFieldOrder = PlayFailReportEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("DtfspEnum".equals(customEnumName))
		{
			customFieldOrder = DtfspEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("DtjsEnum".equals(customEnumName))
		{
			customFieldOrder = DtjsEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("InlinePageEnum".equals(customEnumName))
		{
			customFieldOrder = InlinePageEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("PlayBufferingEnum".equals(customEnumName))
		{
			customFieldOrder = PlayBufferingEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("PlayerBuffEnum".equals(customEnumName))
		{
			customFieldOrder = PlayerBuffEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("PlayHaltDetailEnum".equals(customEnumName))
		{
			customFieldOrder = PlayHaltDetailEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("PlayHaltEnum".equals(customEnumName))
		{
			customFieldOrder = PlayHaltEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("TaskfluxSourceEnum".equals(customEnumName))
		{
			customFieldOrder = TaskfluxSourceEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("HSEnum".equals(customEnumName))
		{
			customFieldOrder = HSEnum.getFieldOrder(customEnumFieldName);
		}
		else if ("LoginEnum".equals(customEnumName))
		{
			customFieldOrder = LoginEnum.getFieldOrder(customEnumFieldName);
		}

		return customFieldOrder;
	}
}
