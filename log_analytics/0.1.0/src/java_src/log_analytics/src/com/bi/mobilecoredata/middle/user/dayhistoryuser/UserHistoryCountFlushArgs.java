package com.bi.mobilecoredata.middle.user.dayhistoryuser;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class UserHistoryCountFlushArgs implements CLPInterface {

	private String[] userHistoryCountFlushParam = null;
	private HashMap<String, Option> hashmap = null;
	private AutoHelpParser parser = null;

	@Override
	public void init(String jarName) throws Exception {
		// TODO Auto-generated method stub
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
	}

	@Override
	public void parse(String[] args) throws Exception {
		// TODO Auto-generated method stub
		parser.parse(args);

		if (args.length == 0
				|| Boolean.TRUE.equals(parser.getOptionValue(hashmap
						.get("help")))) {
			throw new Exception("No args or --help");
		}

		userHistoryCountFlushParam = new String[2];
		userHistoryCountFlushParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
		userHistoryCountFlushParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
		if (userHistoryCountFlushParam[0] == null || userHistoryCountFlushParam[1] == null) {
			throw new Exception("the argument value is null");
		}
	}

	public String[] getUserHistoryCountFlushParam() {
		return userHistoryCountFlushParam;
	}

	public AutoHelpParser getParser() {
		return parser;
	}

}
