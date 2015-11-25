package com.bi.ibidian.jargsparser;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.ibidian.datadefine.CommonEnum;
import com.bi.ibidian.datadefine.CustomEnumNameSet;
import com.bi.ibidian.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;
import com.bi.ibidian.datadefine.CustomEnumNameSet.CustomEnumNotFoundException;

public class CalculateMRArgs implements CLPInterface
{
	private String[] optionValues = null;
	private HashMap<String, Option> optionMap = null;
	private AutoHelpParser autoHelpParser = null;

	@Override
	public void initParserOptions(String jarName) throws Exception
	{
		autoHelpParser = new AutoHelpParser();
		optionMap = new HashMap<String, Option>();

		autoHelpParser.setFuncUsageMsg("hadoop jar " + jarName + " ");

		CmdLineParser.Option argOption = null;
		//@formatter:off
		// 1
		argOption = autoHelpParser.addBooleanOption('h', 
													CommonEnum.HELP.name().toLowerCase());
		autoHelpParser.addOptionHelpMsgs(argOption, 
										 "Return the help message !");
		optionMap.put(CommonEnum.HELP.name().toLowerCase(), 
					  argOption);
		// 2
		argOption = autoHelpParser.addStringOption('i', 
												   CommonEnum.INPUT.name().toLowerCase());
		autoHelpParser.addOptionHelpMsgs(argOption, 
										 "The Hadoop input path that exists currently !");
		optionMap.put(CommonEnum.INPUT.name().toLowerCase(), 
					  argOption);
		// 3
		argOption = autoHelpParser.addStringOption('o', 
												   CommonEnum.OUTPUT.name().toLowerCase());
		autoHelpParser.addOptionHelpMsgs(argOption,
										 "The Hadoop output path that does not exist currently !");
		optionMap.put(CommonEnum.OUTPUT.name().toLowerCase(), 
					  argOption);
		// 4
		argOption = autoHelpParser.addStringOption('e', 
												   CommonEnum.ENUMNAME.name().toLowerCase());
		autoHelpParser.addOptionHelpMsgs(argOption,
				 						 "The given data definition enum that " +
				 						 "identifies the fields of formatted data, " +
				 						 "e.g HotsSpeedEnum !");
		optionMap.put(CommonEnum.ENUMNAME.name().toLowerCase(), 
				      argOption);
		// 5
		argOption = autoHelpParser.addStringOption('d', 
												   CommonEnum.DIMENSIONS.name().toLowerCase());
		autoHelpParser.addOptionHelpMsgs(argOption,
										 "The given dimensions' column name, " +
										 "e.g O_TIMESTAMP,xxx,... And if no dimension" +
										 "then use white space!");
		optionMap.put(CommonEnum.DIMENSIONS.name().toLowerCase(), 
					  argOption);
		// 6
		argOption = autoHelpParser.addStringOption('c', 
												   CommonEnum.INDICATORS.name().toLowerCase());
		autoHelpParser.addOptionHelpMsgs(argOption, 
										 "The given indicators' column name, " +
										 "e.g O_VTIME,xxx,... And if no indicator " +
										 "colunm then use white space!");
		optionMap.put(CommonEnum.INDICATORS.name().toLowerCase(), 
					  argOption);
		//@formatter:on
	}

	@Override
	public void parseArgs(String[] args) throws Exception
	{
		//@formatter:off
		System.out.println("*******print LogArgs parse args*******");
		for (int i = 0; i < args.length; i++)
		{
			System.out.println("args[" + i + "]: " + args[i]);
		}
		System.out.println("**************************************");

		autoHelpParser.parse(args);

		if (args.length == 0 || 
			Boolean.TRUE.equals(
					autoHelpParser.getOptionValue(
							optionMap.get(CommonEnum.HELP.name().toLowerCase()))))
		{
			throw new Exception("No args or --help");
		}
		
		optionValues = new String[5];

		optionValues[0] = (String) autoHelpParser.getOptionValue(
											optionMap.get(CommonEnum.INPUT.name().toLowerCase()));
		optionValues[1] = (String) autoHelpParser.getOptionValue(
											optionMap.get(CommonEnum.OUTPUT.name().toLowerCase()));
		optionValues[2] = (String) autoHelpParser.getOptionValue(
											optionMap.get(CommonEnum.ENUMNAME.name().toLowerCase()));
		optionValues[3] = (String) autoHelpParser.getOptionValue(
											optionMap.get(CommonEnum.DIMENSIONS.name().toLowerCase()));
		optionValues[4] = (String) autoHelpParser.getOptionValue(
											optionMap.get(CommonEnum.INDICATORS.name().toLowerCase()));
		// 1. option value null error
		if (null == optionValues[0] || 
			null == optionValues[1] || 
			null == optionValues[2] ||
			null == optionValues[3] ||
			null == optionValues[4])
		{
			throw new Exception("The argument value is null!");
		}
		//@formatter:on
		// 2. option enumname's value wrong error
		String customEnumName = null;
		try
		{
			customEnumName = CustomEnumNameSet.foundCustomEnumName(optionValues[2]);
		}
		catch (CustomEnumNotFoundException e)
		{
			throw new Exception(e.getMessage());
		}
		// 3. option dimensions' value wrong error
		if (!("".equals(optionValues[3].trim())))
		{
			String[] dimsNameArray = optionValues[3].split(",");
			for (String enumFieldName : dimsNameArray)
			{
				try
				{
					CustomEnumNameSet.getCustomEnumFieldOrder(customEnumName, enumFieldName);
				}
				catch (CustomEnumFieldNotFoundException e)
				{
					throw new Exception(e.getMessage());
				}
			}
		}
		// 3. option indicators' value wrong error
		if (!("".equals(optionValues[4].trim())))
		{
			String[] indsNameArray = optionValues[3].split(",");
			for (String enumFieldName : indsNameArray)
			{
				try
				{
					CustomEnumNameSet.getCustomEnumFieldOrder(customEnumName, enumFieldName);
				}
				catch (CustomEnumFieldNotFoundException e)
				{
					throw new Exception(e.getMessage());
				}
			}
		}

	}

	public String[] getOptionValueArray()
	{
		return optionValues;
	}

	public AutoHelpParser getAutoHelpParser()
	{
		return autoHelpParser;
	}

}
