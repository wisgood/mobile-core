package com.bi.common.init;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

public class CommArgs implements CLPInterface {

    private String[] commsParam = null;

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

    }

    public void parse(String[] args) throws Exception {

//        for (int i = 0; i < args.length; i++) {
//
//            System.out.println(args[i]);
//        }
        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        commsParam = new String[2];
        commsParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        commsParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
        if (commsParam[0] == null || commsParam[1] == null) {
            throw new Exception("the argument value is null");
        }
    }

    public String[] getCommsParam() {
        return commsParam;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
