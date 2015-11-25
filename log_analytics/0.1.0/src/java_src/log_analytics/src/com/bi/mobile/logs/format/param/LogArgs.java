package com.bi.mobile.logs.format.param;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class LogArgs implements CLPInterface {
	private String[] paramStrs = null;
	private HashMap<String, Option> hashmap = null;
	private AutoHelpParser parser = null;
	@Override
	public void init(String jarName) throws Exception {
		// TODO Auto-generated method stub
		parser = new AutoHelpParser();
		hashmap = new HashMap<String, Option>();
		//parser.addFunction("transform ip to area and isp when given column");
		parser.addUsage("hadoop jar "+jarName+" ");
		CmdLineParser.Option help = parser.addHelp(
				parser.addBooleanOption('h',"help"), "return the help doc");
		hashmap.put("help", help);
		CmdLineParser.Option input = parser.addHelp(
				parser.addStringOption('i',"input"), "the input file or path");
		hashmap.put("input", input);
		CmdLineParser.Option output = parser.addHelp(
				parser.addStringOption('o', "output"),"the hadoop result output path,the path does not in the cluster");
		hashmap.put("output", output);
		CmdLineParser.Option files = parser.addHelp(
				parser.addStringOption('f',"files"), "the given confinguration files,e.g ip_table,dm_common_inforhash;");
		hashmap.put("files", files);
	}

	@Override
	public void parse(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("****************print LogArgs parse args*********************");
		for(int i=0;i<args.length;i++){
			System.out.println(args[i]);
		}
		parser.parse(args);

		if (args.length == 0 || Boolean.TRUE.equals(parser.getOptionValue(hashmap.get("help")))) {
			throw new Exception("No args or --help");
		}

		paramStrs = new String[4];
		paramStrs[0] = "-files";
		paramStrs[1] = (String) parser.getOptionValue(hashmap.get("files"));
		System.out.println("filesName:"+paramStrs[1]);
		paramStrs[2] = (String) parser.getOptionValue(hashmap.get("input"));
		paramStrs[3] = (String) parser.getOptionValue(hashmap.get("output"));
		if (paramStrs[1] == null || paramStrs[2] == null || paramStrs[3] == null ) {			
			throw new Exception("the argument value is null");
		}
	}

	public String[] getParamStrs() {
		return paramStrs;
	}

	public AutoHelpParser getParser() {
		return parser;
	}

	

}
