package com.bi.ibidian.jargsparser;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

public class ProprecessMRArgs implements CLPInterface
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

		argOption = autoHelpParser.addBooleanOption('h', "help");
		autoHelpParser.addOptionHelpMsgs(argOption, "Return the help message!");
		optionMap.put("help", argOption);

		argOption = autoHelpParser.addStringOption('i', "input");
		autoHelpParser.addOptionHelpMsgs(argOption, "The Hadoop input path that exists currently!");
		optionMap.put("input", argOption);

		argOption = autoHelpParser.addStringOption('o', "output");
		autoHelpParser.addOptionHelpMsgs(argOption,
				"The Hadoop output path that does not exist currently!");
		optionMap.put("output", argOption);

		argOption = autoHelpParser.addStringOption('f', "files");
		autoHelpParser.addOptionHelpMsgs(argOption, "The given dimension files, e.g xxx,xxx,xxx!");
		optionMap.put("files", argOption);
	}

	@Override
	public void parseArgs(String[] args) throws Exception
	{
		System.out.println("*******print LogArgs parse args*******");
		for (int i = 0; i < args.length; i++)
		{
			System.out.println("args[" + i + "]: " + args[i]);
		}
		System.out.println("**************************************");

		autoHelpParser.parse(args);

		if (args.length == 0
				|| Boolean.TRUE.equals(autoHelpParser.getOptionValue(optionMap.get("help"))))
		{
			throw new Exception("No args or --help");
		}

		optionValues = new String[4];
		optionValues[0] = "-files";
		optionValues[1] = (String) autoHelpParser.getOptionValue(optionMap.get("files"));
		System.out.println("filesName:" + optionValues[1]);
		optionValues[2] = (String) autoHelpParser.getOptionValue(optionMap.get("input"));
		optionValues[3] = (String) autoHelpParser.getOptionValue(optionMap.get("output"));
		if (optionValues[1] == null || optionValues[2] == null || optionValues[3] == null)
		{
			throw new Exception("the argument value is null");
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
