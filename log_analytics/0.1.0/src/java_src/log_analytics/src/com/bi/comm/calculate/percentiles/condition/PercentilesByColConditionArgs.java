package com.bi.comm.calculate.percentiles.condition;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class PercentilesByColConditionArgs implements CLPInterface {
    private String[] percentilesConditionParam = null;

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

        CmdLineParser.Option groupby = parser.addHelp(
                parser.addStringOption('g', "groupby"),
                "the given group by colum,e.g 1,2,3");
        hashmap.put("groupby", groupby);

        CmdLineParser.Option percent = parser.addHelp(
                parser.addStringOption('p', "percent"),
                "the given percent ,e.g 0.75");
        hashmap.put("percent", percent);

        CmdLineParser.Option pncolindex = parser.addHelp(
                parser.addStringOption('x', "pncolindex"),
                "the given percentile cacul index by colum ,e.g 4");
        hashmap.put("pncolindex", pncolindex);

        CmdLineParser.Option conditioncol = parser.addHelp(
                parser.addStringOption('t', "conditioncol"),
                "the given condition colum ,e.g 3");
        hashmap.put("conditioncol", conditioncol);

        CmdLineParser.Option conditionvalue = parser.addHelp(
                parser.addStringOption('u', "conditionvalue"),
                "the given condition value index by colum ,e.g 2");
        hashmap.put("conditionvalue", conditionvalue);

    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        percentilesConditionParam = new String[7];
        percentilesConditionParam[0] = (String) parser.getOptionValue(hashmap
                .get("input"));
        percentilesConditionParam[1] = (String) parser.getOptionValue(hashmap
                .get("output"));
        percentilesConditionParam[2] = (String) parser.getOptionValue(hashmap
                .get("groupby"));
        percentilesConditionParam[3] = (String) parser.getOptionValue(hashmap
                .get("percent"));
        percentilesConditionParam[4] = (String) parser.getOptionValue(hashmap
                .get("pncolindex"));
        percentilesConditionParam[5] = (String) parser.getOptionValue(hashmap
                .get("conditioncol"));
        percentilesConditionParam[6] = (String) parser.getOptionValue(hashmap
                .get("conditionvalue"));
        if (percentilesConditionParam[0] == null
                || percentilesConditionParam[1] == null
                || percentilesConditionParam[2] == null
                || percentilesConditionParam[3] == null
                || percentilesConditionParam[4] == null
                || percentilesConditionParam[5] == null
                || percentilesConditionParam[6] == null) {
            throw new Exception("the argument value is null");
        }

        if (percentilesConditionParam[5].split(",").length != percentilesConditionParam[6]
                .split(",").length) {
            throw new Exception(
                    "the condition index count does not match condition value count!");

        }
    }

    public AutoHelpParser getParser() {
        return parser;
    }

    public String[] getPercentilesParam() {
        return percentilesConditionParam;
    }
}
