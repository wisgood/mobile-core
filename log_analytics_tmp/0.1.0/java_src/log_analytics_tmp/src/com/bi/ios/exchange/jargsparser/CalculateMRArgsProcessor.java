package com.bi.ios.exchange.jargsparser;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.ios.exchange.datadefine.CommonEnum;
import com.bi.ios.exchange.datadefine.CustomEnumNameSet;
import com.bi.ios.exchange.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;
import com.bi.ios.exchange.datadefine.CustomEnumNameSet.CustomEnumNotFoundException;

public class CalculateMRArgsProcessor implements MRArgsProcessorInterface
{
	private String[] optionsValue = null;
	private HashMap<String, Option> optionsMap = null;
	private MROptionsParser mrOptionsParser = null;

	@Override
	public void initDefaultOptions(String jarName) throws Exception
	{
		mrOptionsParser = new MROptionsParser();
		optionsMap = new HashMap<String, Option>();

		mrOptionsParser.setJarName(jarName);

		CmdLineParser.Option argOption = null;
		//@formatter:off
		// 1.--help
		argOption = mrOptionsParser.addBooleanOption('h', CommonEnum.HELP.name().toLowerCase());
		mrOptionsParser.setOptionDescription(argOption, "Return the help message !");
		optionsMap.put(CommonEnum.HELP.name().toLowerCase(), argOption);
		// 2.--input
		argOption = mrOptionsParser.addStringOption('i', CommonEnum.INPUT.name().toLowerCase());
		mrOptionsParser.setOptionDescription(argOption, "The Hadoop input path that exists currently !");
		optionsMap.put(CommonEnum.INPUT.name().toLowerCase(), argOption);
		// 3.--output
		argOption = mrOptionsParser.addStringOption('o', CommonEnum.OUTPUT.name().toLowerCase());
		mrOptionsParser.setOptionDescription(argOption, "The Hadoop output path that does not exist " +
														"currently !");
		optionsMap.put(CommonEnum.OUTPUT.name().toLowerCase(), argOption);
		// 4.--enumname
		argOption = mrOptionsParser.addStringOption('e', CommonEnum.ENUMNAME.name().toLowerCase());
		mrOptionsParser.setOptionDescription(argOption, "The given data definition enum that " +
				 						 				"identifies the fields of formatted data, " +
				 						 				"e.g HotsSpeedEnum !");
		optionsMap.put(CommonEnum.ENUMNAME.name().toLowerCase(), argOption);
		// 5.--dimensions
		argOption = mrOptionsParser.addStringOption('d', CommonEnum.DIMENSIONS.name().toLowerCase());
		mrOptionsParser.setOptionDescription(argOption, "The given dimensions' column name, " +
										 				"e.g O_TIMESTAMP,xxx,... And if no dimension" +
										 				"then use white space!");
		optionsMap.put(CommonEnum.DIMENSIONS.name().toLowerCase(), argOption);
		// 6.--distindicators
		argOption = mrOptionsParser.addStringOption('p', CommonEnum.DISTINDICATORS.name().toLowerCase());
		mrOptionsParser.setOptionDescription(argOption, "The given distinct indicators' column name, " +
										 				"e.g O_FCK,xxx,... And if no indicator " +
										 				"colunm then use white space!");
		optionsMap.put(CommonEnum.DISTINDICATORS.name().toLowerCase(), argOption);
		
		//@formatter:on
	}

	@Override
	public void parseAndCheckArgs(String[] args) throws Exception
	{
		//@formatter:off
		System.out.println("*******ALL THE GIVEN ARGS*******");
		for (int i = 0; i < args.length; i++)
		{
			System.out.println("args[" + i + "]: " + args[i]);
		}
		System.out.println("********************************");

		// 从所有参数中把选项、值分两部分解析出来
		mrOptionsParser.parse(args);

		if (args.length == 0 || Boolean.TRUE.equals(
									mrOptionsParser.getOptionValue(
											optionsMap.get(CommonEnum.HELP.name().toLowerCase()))))
		{
			throw new Exception("No args nor --help !");
		}
		
		optionsValue = new String[5];

		optionsValue[0] = (String) mrOptionsParser.getOptionValue(
											optionsMap.get(CommonEnum.INPUT.name().toLowerCase()));
		optionsValue[1] = (String) mrOptionsParser.getOptionValue(
											optionsMap.get(CommonEnum.OUTPUT.name().toLowerCase()));
		optionsValue[2] = (String) mrOptionsParser.getOptionValue(
											optionsMap.get(CommonEnum.ENUMNAME.name().toLowerCase()));
		optionsValue[3] = (String) mrOptionsParser.getOptionValue(
											optionsMap.get(CommonEnum.DIMENSIONS.name().toLowerCase()));
		optionsValue[4] = (String) mrOptionsParser.getOptionValue(
											optionsMap.get(CommonEnum.DISTINDICATORS.name().toLowerCase()));

		// 1. lack of some options and their values error
		if (null == optionsValue[0] || 
			null == optionsValue[1] || 
			null == optionsValue[2] || 
			null == optionsValue[3] || 
			null == optionsValue[4])
		{
			StringBuilder optionsList = new StringBuilder();
			for (String optionName : optionsMap.keySet())
			{
				optionsList.append("'");
				optionsList.append("--");
				optionsList.append(optionName);
				optionsList.append("'");
			}
			throw new Exception("Lack of some of the following options and their values:" + 
								optionsList.toString());
		}
		//@formatter:on

		// 2. option enumname's value wrong error
		String customEnumName = null;
		String[] enumFieldsName = null;
		try
		{
			customEnumName = CustomEnumNameSet.foundCustomEnumName(optionsValue[2]);
		}
		catch (CustomEnumNotFoundException e)
		{
			throw new Exception(e.getMessage());
		}

		// 3. option dimensions' value wrong error
		if (!("".equals(optionsValue[3].trim())))
		{
			enumFieldsName = optionsValue[3].split(",");

			for (String fieldName : enumFieldsName)
			{
				try
				{
					CustomEnumNameSet.getCustomEnumFieldOrder(customEnumName, fieldName);
				}
				catch (CustomEnumFieldNotFoundException e)
				{
					throw new Exception(e.getMessage());
				}
			}
		}

		// 4. option distinct indicators' value wrong error
		if (!("".equals(optionsValue[4].trim())))
		{
			enumFieldsName = optionsValue[4].split(",");

			for (String fieldName : enumFieldsName)
			{
				try
				{
					CustomEnumNameSet.getCustomEnumFieldOrder(customEnumName, fieldName);
				}
				catch (CustomEnumFieldNotFoundException e)
				{
					throw new Exception(e.getMessage());
				}
			}
		}
	}

	public String[] getOptionsValueArray()
	{
		return optionsValue;
	}

	public MROptionsParser getMapRedOptionsParser()
	{
		return mrOptionsParser;
	}

}
