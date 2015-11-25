package com.bi.minisite.jargsparser;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

public class PreprocessMRArgsProcessor implements MRArgsProcessorInterface
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

		argOption = mrOptionsParser.addBooleanOption('h', "help");
		mrOptionsParser.setOptionDescription(argOption, "Return the help message!");
		optionsMap.put("help", argOption);

		argOption = mrOptionsParser.addStringOption('i', "input");
		mrOptionsParser.setOptionDescription(argOption, "The Hadoop input path that exists currently!");
		optionsMap.put("input", argOption);

		argOption = mrOptionsParser.addStringOption('o', "output");
		mrOptionsParser.setOptionDescription(argOption,
				"The Hadoop output path that does not exist currently!");
		optionsMap.put("output", argOption);

		argOption = mrOptionsParser.addStringOption('f', "files");
		mrOptionsParser.setOptionDescription(argOption, "The given dimension files, e.g xxx,xxx,xxx!");
		optionsMap.put("files", argOption);
	}

	@Override
	public void parseAndCheckArgs(String[] args) throws Exception
	{
		System.out.println("*******print LogArgs parse args*******");
		for (int i = 0; i < args.length; i++)
		{
			System.out.println("args[" + i + "]: " + args[i]);
		}
		System.out.println("**************************************");

		mrOptionsParser.parse(args);

		if (args.length == 0
				|| Boolean.TRUE.equals(mrOptionsParser.getOptionValue(optionsMap.get("help"))))
		{
			throw new Exception("No args or --help");
		}

		optionsValue = new String[4];
		optionsValue[0] = "-files";
		optionsValue[1] = (String) mrOptionsParser.getOptionValue(optionsMap.get("files"));
		System.out.println("filesName:" + optionsValue[1]);
		optionsValue[2] = (String) mrOptionsParser.getOptionValue(optionsMap.get("input"));
		optionsValue[3] = (String) mrOptionsParser.getOptionValue(optionsMap.get("output"));
		if (optionsValue[1] == null || optionsValue[2] == null || optionsValue[3] == null)
		{
			throw new Exception("the argument value is null");
		}
	}

	public String[] getOptionValueArray()
	{
		return optionsValue;
	}

	public MROptionsParser getAutoHelpParser()
	{
		return mrOptionsParser;
	}

}
