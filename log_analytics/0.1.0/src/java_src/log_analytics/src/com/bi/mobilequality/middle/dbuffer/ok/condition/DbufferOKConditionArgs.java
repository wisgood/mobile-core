package com.bi.mobilequality.middle.dbuffer.ok.condition;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class DbufferOKConditionArgs implements CLPInterface {

    private String[] okParam = null;

    private HashMap<String, Option> hashmap = null;

    private AutoHelpParser parser = null;

    @Override
    public void init(String jarName) throws Exception {
        // TODO Auto-generated method stub
        hashmap = new HashMap<String, Option>();
        parser = new AutoHelpParser();
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

        CmdLineParser.Option ok = parser.addHelp(
                parser.addStringOption('k', "ok"), "the given ok,e.g 1,2,3");
        hashmap.put("ok", ok);
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

        okParam = new String[3];
        okParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        okParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
        okParam[2] = (String) parser.getOptionValue(hashmap.get("ok"));
        if (okParam[0] == null || okParam[1] == null || okParam[2] == null) {
            throw new Exception("the argument value is null");
        }
    }

    public String[] getOkParam() {
        return okParam;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
