package com.bi.calculate.common;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.common.init.AutoHelpParser;
import com.bi.common.init.CLPInterface;

public class ExtractAndCountParaArgs implements CLPInterface {

    private String[] countParam = null;

    private HashMap<String, Option> hashmap = null;

    private com.bi.common.init.AutoHelpParser parser = null;

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

        CmdLineParser.Option column = parser.addHelp(
                parser.addStringOption('c', "column"),
                "the given column,e.g 0,1,2,3");
        hashmap.put("column", column);
        
        CmdLineParser.Option colid = parser.addHelp(
                parser.addStringOption('l', "colid"),
                "the given colid,e.g 1,0");
        hashmap.put("colid", colid);
        
        CmdLineParser.Option colvalue = parser.addHelp(
                parser.addStringOption('v', "colvalue"),
                "the given colvalue,e.g 1,2,3,4");
        hashmap.put("colvalue", colvalue);

    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        countParam = new String[5];
        countParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        countParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
        countParam[2] = (String) parser.getOptionValue(hashmap.get("column"));
        countParam[3] = (String) parser.getOptionValue(hashmap.get("colid"));
        countParam[4] = (String) parser.getOptionValue(hashmap.get("colvalue"));
        if (countParam[0] == null || countParam[1] == null
                || countParam[2] == null || countParam[3] == null|| countParam[4] == null) {
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
