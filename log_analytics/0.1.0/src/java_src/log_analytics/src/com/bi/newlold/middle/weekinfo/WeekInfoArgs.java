package com.bi.newlold.middle.weekinfo;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class WeekInfoArgs implements CLPInterface {

    private String[] weekInfoParam = null;

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
        CmdLineParser.Option weeksday = parser
                .addHelp(parser.addStringOption('w', "weeksday"),
                        "the hadoop result output path,the path does not in the cluster");
        hashmap.put("weeksday", weeksday);

    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        weekInfoParam = new String[3];
        weekInfoParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        weekInfoParam[1] = (String) parser
                .getOptionValue(hashmap.get("output"));
        weekInfoParam[2] = (String) parser.getOptionValue(hashmap
                .get("weeksday"));
        if (weekInfoParam[0] == null || weekInfoParam[1] == null
                || null == weekInfoParam[2]) {
            throw new Exception("the argument value is null");
        }
    }

    public String[] getCommsParam() {
        return weekInfoParam;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
