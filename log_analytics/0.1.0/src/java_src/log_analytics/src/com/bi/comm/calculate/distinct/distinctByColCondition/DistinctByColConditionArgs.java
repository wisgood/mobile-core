package com.bi.comm.calculate.distinct.distinctByColCondition;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class DistinctByColConditionArgs implements CLPInterface {
    private String[] distinctConditionParam = null;

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

        CmdLineParser.Option distinctcolindex = parser.addHelp(
                parser.addStringOption('d', "distinctcolindex"),
                "the given distinct column index ,e.g 4");
        hashmap.put("distinctcolindex", distinctcolindex);

        CmdLineParser.Option conditioncol = parser.addHelp(
                parser.addStringOption('c', "conditioncol"),
                "the given condition colum ,e.g 3");
        hashmap.put("conditioncol", conditioncol);

        CmdLineParser.Option conditionvalue = parser.addHelp(
                parser.addStringOption('v', "conditionvalue"),
                "the given condition value index by colum ,e.g 2");
        hashmap.put("conditionvalue", conditionvalue);
        
        CmdLineParser.Option separator = parser.addHelp(
                parser.addStringOption('s', "separator"),
                "the given separator characater by colum ,e.g , \t, \t default");
        hashmap.put("separator", separator);
        
        CmdLineParser.Option decode = parser.addHelp(
                parser.addStringOption('e', "decode"),
                "the given Input decode,e.g:lzo,text, lzo default");
        hashmap.put("decode", decode);

    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        distinctConditionParam = new String[]{null, null, null, null, null, "\t", "text"};
        distinctConditionParam[0] = (String) parser.getOptionValue(hashmap
                .get("input"));
        distinctConditionParam[1] = (String) parser.getOptionValue(hashmap
                .get("output"));
        distinctConditionParam[2] = (String) parser.getOptionValue(hashmap
                .get("distinctcolindex"));
        distinctConditionParam[3] = (String) parser.getOptionValue(hashmap
                .get("conditioncol"));
        
        distinctConditionParam[4] = (String) parser.getOptionValue(hashmap
                .get("conditionvalue"));
        
        String separator = (String) parser.getOptionValue(hashmap.get("separator"));
        if (separator != null){
        	distinctConditionParam[5] = separator;
        }
        
        String decode = (String) parser.getOptionValue(hashmap.get("decode"));
        if (decode != null){
        	distinctConditionParam[6] = decode;
        }
        
        if (distinctConditionParam[0] == null
                || distinctConditionParam[1] == null
                || distinctConditionParam[2] == null
                || distinctConditionParam[3] == null){
            throw new Exception("the argument value is null");
        }
     
        if (distinctConditionParam[3].split(",").length != distinctConditionParam[4]
                .split(",").length) {
            throw new Exception(
                    "the condition index count does not match condition value count!");

        }
    }

    public AutoHelpParser getParser() {
        return parser;
    }

    public String[] getParams() {
        return distinctConditionParam;
    }
}
