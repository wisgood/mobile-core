package com.bi.ibidian.jargsparser;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

public class MapRedArgs implements CLPInterface
{
	private String[] paramStrs = null;
	private HashMap<String, Option> hashmap = null;
	private AutoHelpParser parser = null;

	@Override
	public void initParserOptions(String jarName) throws Exception
	{
		parser = new AutoHelpParser();

		hashmap = new HashMap<String, Option>();

		parser.setFuncUsageMsg("hadoop jar " + jarName + " ");

		CmdLineParser.Option argOption = null;

		argOption = parser.addBooleanOption('h', "help");
		parser.addOptionHelpMsgs(argOption, "Return the help message!");
		hashmap.put("help", argOption);

		argOption = parser.addStringOption('i', "input");
		parser.addOptionHelpMsgs(argOption, "The Hadoop input path that exists currently!");
		hashmap.put("input", argOption);

		argOption = parser.addStringOption('o', "output");
		parser.addOptionHelpMsgs(argOption, "The Hadoop output path that does not exist currently!");
		hashmap.put("output", argOption);

		argOption = parser.addStringOption('f', "files");
		parser.addOptionHelpMsgs(argOption, "The given dimension files, e.g xxx,xxx,xxx!");
		hashmap.put("files", argOption);
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

		parser.parse(args);

		if (args.length == 0 || Boolean.TRUE.equals(parser.getOptionValue(hashmap.get("help"))))
		{
			throw new Exception("No args or --help");
		}

		paramStrs = new String[4];
		paramStrs[0] = "-files";
		paramStrs[1] = (String) parser.getOptionValue(hashmap.get("files"));
		System.out.println("filesName:" + paramStrs[1]);
		paramStrs[2] = (String) parser.getOptionValue(hashmap.get("input"));
		paramStrs[3] = (String) parser.getOptionValue(hashmap.get("output"));
		if (paramStrs[1] == null || paramStrs[2] == null || paramStrs[3] == null)
		{
			throw new Exception("the argument value is null");
		}
	}

	public String[] getOptionValueArray()
	{
		return paramStrs;
	}

	public AutoHelpParser getAutoHelpParser()
	{
		return parser;
	}

}
