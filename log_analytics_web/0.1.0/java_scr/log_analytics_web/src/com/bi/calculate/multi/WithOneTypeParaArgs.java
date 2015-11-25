package com.bi.calculate.multi;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.common.init.AutoHelpParser;
import com.bi.common.init.CLPInterface;

public class WithOneTypeParaArgs implements CLPInterface {

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
                "the given column,e.g 1,2,3");
        hashmap.put("column", column);
        
        CmdLineParser.Option type = parser.addHelp(
                parser.addStringOption('t', "type"),
                "the given type,e.g 1,0");
        hashmap.put("type", type);

    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        countParam = new String[4];
        countParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        countParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
        countParam[2] = (String) parser.getOptionValue(hashmap.get("column"));
        countParam[3] = (String) parser.getOptionValue(hashmap.get("type"));
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
