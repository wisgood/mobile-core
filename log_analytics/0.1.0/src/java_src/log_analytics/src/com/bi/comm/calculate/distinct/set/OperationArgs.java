package com.bi.comm.calculate.distinct.set;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class OperationArgs implements CLPInterface {

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
        
        CmdLineParser.Option left = parser
                .addHelp(parser.addStringOption('l', "left"),
                        "the left input path identify");
        hashmap.put("left", left);
        
        CmdLineParser.Option column = parser
                .addHelp(parser.addStringOption('c', "column"),
                        "the column fields to do set operation");
        hashmap.put("column", column);
        
        CmdLineParser.Option operation = parser
                .addHelp(parser.addStringOption('p', "operation"),
                        "the set operation you want do, eg AND, OR, SUB ");
        hashmap.put("operation", operation);
        
        CmdLineParser.Option separator = parser.addHelp(
                parser.addStringOption('s', "separator"),
                "the given separator characater by colum, e.g:\t, default \t");
        hashmap.put("separator", separator);

        CmdLineParser.Option decode = parser.addHelp(
                parser.addStringOption('d', "decode"),
                "the given Input Decodec type,e.g:lzo,text default text");
        hashmap.put("decode", decode);
    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        countParam = new String[]{null, null, null, null, null, "\t", "text"};
        
        countParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        countParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
        countParam[2] = (String) parser.getOptionValue(hashmap.get("left"));
        countParam[3] = (String) parser.getOptionValue(hashmap.get("column"));
        countParam[4] = (String) parser.getOptionValue(hashmap.get("operation"));
        
		String separator = (String) parser.getOptionValue(hashmap
				.get("separator"));
		if (separator != null) {
			countParam[5] = separator;
		}

		String decode = (String) parser.getOptionValue(hashmap.get("decode"));
		if (decode != null) {
			countParam[6] = decode;
		}

        if (countParam[0] == null || countParam[1] == null
                || countParam[2] == null || countParam[3] == null) {
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
