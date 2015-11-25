package com.bi.comm.calculate.distinct;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class DistinctByColArgs implements CLPInterface {

    private String[] distinctParam = null;

    private HashMap<String, Option> hashmap = null;

    private AutoHelpParser parser = null;

    @Override
    public void init(String jarName) throws Exception {

        parser = new AutoHelpParser();
        hashmap = new HashMap<String, Option>();

        parser.addFunction("Calculate distinct count when given column");
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

        CmdLineParser.Option distbycolum = parser.addHelp(
                parser.addStringOption('d', "distbycolum"),
                "the given distbycolum,e.g 1");
        hashmap.put("distbycolum", distbycolum);
        CmdLineParser.Option delim = parser.addHelp(
                parser.addStringOption('e', "delim"),
                "the given delim,e.g a(\t),b(,)");
        hashmap.put("delim", delim);

    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        distinctParam = new String[5];
        distinctParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        distinctParam[1] = (String) parser
                .getOptionValue(hashmap.get("output"));
        distinctParam[2] = (String) parser
                .getOptionValue(hashmap.get("column"));

        distinctParam[3] = (String) parser.getOptionValue(hashmap
                .get("distbycolum"));
        distinctParam[4] = (String) parser.getOptionValue(hashmap.get("delim"));

        if (distinctParam[0] == null || distinctParam[1] == null
                || distinctParam[2] == null || distinctParam[3] == null
                || distinctParam[4] == null) {
            throw new Exception("the argument value is null");
        }
        if ("a".equalsIgnoreCase(distinctParam[4])) {
            distinctParam[4] = "\t";
        }
        else if ("b".equalsIgnoreCase(distinctParam[4])) {
            distinctParam[4] = ",";

        }
    }

    public String[] getDistinctParam() {
        return distinctParam;
    }

    public HashMap<String, Option> getHashmap() {
        return hashmap;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
