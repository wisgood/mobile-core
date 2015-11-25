package com.bi.comm.calculate.sumbycol;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;
import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class SumColArgs implements CLPInterface {

	private String[] sumParam = null;
	private HashMap<String, Option> hashmap = null;
	private AutoHelpParser parser = null;

	// sum.jar
	@Override
	public void init(String jarName) throws Exception {

		parser = new AutoHelpParser();
		hashmap = new HashMap<String, Option>();

		parser.addFunction("Calculate Sum of the given column");
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

		CmdLineParser.Option column = parser.addHelp(
				parser.addStringOption('c', "column"),
				"the given column,e.g 1,2,3");
		hashmap.put("column", column);
		
		CmdLineParser.Option sumcol = parser.addHelp(
				parser.addStringOption('s',"sumcol"), "the given column,e.g vtm");
		hashmap.put("sumcol", sumcol);

	}

	public void parse(String[] args) throws Exception {

		parser.parse(args);

		if (args.length == 0
				|| Boolean.TRUE.equals(parser.getOptionValue(hashmap
						.get("help")))) {
			throw new Exception("No args or --help");
		}

		sumParam = new String[4];
		sumParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
		sumParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
		sumParam[2] = (String) parser.getOptionValue(hashmap.get("column"));
		sumParam[3] = ((String) parser.getOptionValue(hashmap.get("sumcol")));
		
		if (sumParam[0] == null || sumParam[1] == null || sumParam[2] == null|| sumParam[3] == null) {
			throw new Exception("the argument value is null");
		}
	}

	public String[] getSumParam() {
		return sumParam;
	}

}
