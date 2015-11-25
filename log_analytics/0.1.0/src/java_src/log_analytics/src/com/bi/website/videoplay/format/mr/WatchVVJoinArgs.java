package com.bi.website.videoplay.format.mr;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class WatchVVJoinArgs implements CLPInterface {
	private String[] countParam = null;
	private HashMap<String, Option> hashmap = null;
	private AutoHelpParser parser = null;

	@Override
	public void init(String jarName) throws Exception {

		parser = new AutoHelpParser();
		hashmap = new HashMap<String, Option>();

		parser.addFunction("Calculate number of the file records");
		parser.addUsage("hadoop jar " + jarName + " ");

		CmdLineParser.Option help = parser.addHelp(
				parser.addBooleanOption('h', "help"), "return the help doc");
		hashmap.put("help", help);

		CmdLineParser.Option input = parser.addHelp(
				parser.addStringOption('i', "input"), "the input file or path");
		hashmap.put("input", input);

		CmdLineParser.Option output = parser
				.addHelp(parser.addStringOption('o', "output"),
						"the hadoop result output path,the path does not in the cluster");
		hashmap.put("output", output);
		
		CmdLineParser.Option flags = parser.addHelp(
				parser.addStringOption('g', "flags"),
				"the given type,only 1 or 2");
		hashmap.put("flags", flags);		
	}

	public void parse(String[] args) throws Exception {

		parser.parse(args);

		if (args.length == 0
				|| Boolean.TRUE.equals(parser.getOptionValue(hashmap
						.get("help")))) {
			throw new Exception("No args or --help");
		}

		countParam = new String[3];
		countParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
		countParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
		countParam[2] = (String) parser.getOptionValue(hashmap.get("flags"));		
		
		if (countParam[0] == null || countParam[1] == null|| countParam[2] == null) {
			throw new Exception("the argument value is null");
		}
	}

	public String[] getCountParam() {
		return countParam;
	}

	public AutoHelpParser getParser() {
		return parser;
	}


}
