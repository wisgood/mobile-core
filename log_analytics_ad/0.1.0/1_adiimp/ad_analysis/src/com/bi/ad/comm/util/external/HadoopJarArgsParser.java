package com.bi.ad.comm.util.external;

import java.util.HashMap;
import java.util.ArrayList;
import com.bi.ad.comm.util.external.CmdLineParser;
import com.bi.ad.comm.util.external.CmdLineParser.Option;

public class HadoopJarArgsParser  {
	private ArrayList<String> paramStrs = null;
	private HashMap<String, Option> hashmap = null;
	private AutoHelpParser parser = null;


	public void init(String jarName) throws Exception {
		
		parser = new AutoHelpParser();
		hashmap = new HashMap<String, Option>();

		parser.addUsage("hadoop MR class " + jarName + " ");
		CmdLineParser.Option help = parser.addHelp(
				parser.addBooleanOption('h', "help"), "return the help doc");
		hashmap.put("help", help);
		
		CmdLineParser.Option input = parser.addHelp(
				parser.addStringOption('i', "input"), "the input file or path");
		hashmap.put("input", input);
		
		CmdLineParser.Option output = parser.addHelp(
				parser.addStringOption('o', "output"), "the hadoop result output path,the path does not in the cluster");
		hashmap.put("output", output);
		
		CmdLineParser.Option files = parser.addHelp(
				parser.addStringOption('f', "files"), "the given confinguration files,e.g ip_table,dm_common_inforhash");
		hashmap.put("files", files);
	}


	public void parse(String[] args) throws Exception {
		parser.parse(args);
		paramStrs = new ArrayList<String>();
		if (args.length == 0
				|| Boolean.TRUE.equals(parser.getOptionValue(hashmap.get("help")))) 
			throw new Exception("No args or --help");
		
		for (String arg : args){
			if(arg.equals("-f") || arg.equals("--files")){
				paramStrs.add("-files");
				paramStrs.add((String) parser.getOptionValue(hashmap.get("files")));
				if(null == paramStrs.get(1))
					throw new Exception("the argument value is null");
			}
		}	
		paramStrs.add((String) parser.getOptionValue(hashmap.get("input")));
		paramStrs.add((String) parser.getOptionValue(hashmap.get("output")));
		if (paramStrs.size() !=2 && paramStrs.size() != 4 ) 
			throw new Exception("the argument1 value is null");		
	}

	public String[] getParamStrs() {
		String[] tmp = new String[4];
		return paramStrs.toArray(tmp);
	}

	public AutoHelpParser getParser() {
		return parser;
	}
		
}
