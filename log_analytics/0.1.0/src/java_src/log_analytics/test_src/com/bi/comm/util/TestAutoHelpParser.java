package com.bi.comm.util;

import java.util.HashMap;

import jargs.gnu.CmdLineParser.IllegalOptionValueException;
import jargs.gnu.CmdLineParser.UnknownOptionException;
import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;


public class TestAutoHelpParser {

	
	
	public static void main(String[] args){
		AutoHelpParser parser  = new AutoHelpParser();
		HashMap<String, Option> hashmap = new HashMap<String, Option>();
		
		parser.addFunction("transform ip to area and isp when given column");
		parser.addUsage("hadoop jar ip2area.jar ");
		
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
				parser.addStringOption('f',"files"), "the given confinguration files,e.g ip_table;");
		hashmap.put("files", files);
		System.out.println(hashmap);
		try {
			parser.parse(args);
			String[] ip2AreaParam = new String[4];
			ip2AreaParam[0] = "-files";
			ip2AreaParam[1] = (String) parser.getOptionValue(hashmap.get("files"));		
			ip2AreaParam[2] = (String) parser.getOptionValue(hashmap.get("input"));
			ip2AreaParam[3] = (String) parser.getOptionValue(hashmap.get("output"));
			for (int i=0;i<ip2AreaParam.length;i++){
				System.out.println(ip2AreaParam[i]);
				
			}
		} catch (IllegalOptionValueException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownOptionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

}
