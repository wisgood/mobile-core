package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class FsPalyAfterJoinByMACArgs implements CLPInterface {

    private String[] fspalyBymacParam = null;

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

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        fspalyBymacParam = new String[2];
        fspalyBymacParam[0] = (String) parser.getOptionValue(hashmap
                .get("input"));
        fspalyBymacParam[1] = (String) parser.getOptionValue(hashmap
                .get("output"));
        if (fspalyBymacParam[0] == null || fspalyBymacParam[1] == null) {
            throw new Exception("the argument value is null");
        }
    }

    public String[] getCountParam() {
        return fspalyBymacParam;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
