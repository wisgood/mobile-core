package com.bi.chineseidol;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.common.init.AutoHelpParser;
import com.bi.common.init.CLPInterface;

public class PVMuvsConditionArgs implements CLPInterface {

    private String[] pvmuvsParam = null;

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

        CmdLineParser.Option curl = parser.addHelp(
                parser.addStringOption('r', "curl"),
                "the given curl,http://localhost/");
        hashmap.put("curl", curl);
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

        pvmuvsParam = new String[3];
        pvmuvsParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        pvmuvsParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
        pvmuvsParam[2] = (String) parser.getOptionValue(hashmap.get("curl"));
        if (pvmuvsParam[0] == null || pvmuvsParam[1] == null
                || pvmuvsParam[2] == null) {
            throw new Exception("the argument value is null");
        }
    }

    public String[] getOkParam() {
        return pvmuvsParam;
    }

    public AutoHelpParser getParser() {
        return parser;
    }
}
