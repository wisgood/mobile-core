package com.bi.comm.calculate.extract;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class ExtractByColArgs implements CLPInterface {

    private String[] extractParam = null;

    private HashMap<String, Option> hashmap = null;

    private AutoHelpParser parser = null;

    @Override
    public void init(String jarName) throws Exception {

        parser = new AutoHelpParser();
        hashmap = new HashMap<String, Option>();

        parser.addFunction("extract the given columns");
        parser.addUsage("hadoop jar extract.jar ");

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

        CmdLineParser.Option orderbycolum = parser.addHelp(
                parser.addStringOption('b', "orderbycolum"),
                "the given orderbycolum,e.g 1");
        hashmap.put("orderbycolum", orderbycolum);
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

        extractParam = new String[5];
        extractParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        extractParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
        extractParam[2] = (String) parser.getOptionValue(hashmap.get("column"));
        extractParam[3] = (String) parser.getOptionValue(hashmap
                .get("orderbycolum"));
        extractParam[4] = (String) parser.getOptionValue(hashmap.get("delim"));
        if (extractParam[0] == null || extractParam[1] == null
                || extractParam[2] == null || extractParam[3] == null
                || extractParam[4] == null) {
            throw new Exception("the argument value is null");
        }
        if ("a".equalsIgnoreCase(extractParam[4])) {
            extractParam[4] = "\t";
        }
        else if ("b".equalsIgnoreCase(extractParam[4])) {
            extractParam[4] = ",";

        }
    }

    public String[] getExtractParam() {
        return extractParam;
    }

    public HashMap<String, Option> getHashmap() {
        return hashmap;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
