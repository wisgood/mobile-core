package com.bi.comm.calculate.proportion;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class ProportionByColArgs implements CLPInterface {

    private String[] proportsParam = null;

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

        CmdLineParser.Option numerator = parser
                .addHelp(parser.addStringOption('n', "numerator"),
                        "the given numerator name ,e.g bootstrap_date_plat_version_btype");
        hashmap.put("numerator", numerator);

        CmdLineParser.Option cacul = parser
                .addHelp(parser.addStringOption('c', "cacul"),
                        "the given cacul by colum ,e.g bootstrap_date_plat_version_btype");
        hashmap.put("cacul", cacul);
    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        proportsParam = new String[5];
        proportsParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        proportsParam[1] = (String) parser
                .getOptionValue(hashmap.get("output"));
        proportsParam[2] = (String) parser.getOptionValue(hashmap
                .get("groupby"));
        proportsParam[3] = (String) parser.getOptionValue(hashmap
                .get("numerator"));
        proportsParam[4] = (String) parser.getOptionValue(hashmap.get("cacul"));
        if (proportsParam[0] == null || proportsParam[1] == null
                || proportsParam[2] == null || proportsParam[3] == null
                || proportsParam[4] == null ) {
            throw new Exception("the argument value is null");
        }
    }

    public String[] getProportionParam() {
        return proportsParam;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
