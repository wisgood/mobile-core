package com.bi.common.etl;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.common.init.AutoHelpParser;
import com.bi.common.init.CLPInterface;

public class LogFormatArgs implements CLPInterface {

    private String[] params = null;

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

        CmdLineParser.Option files = parser.addHelp(
                parser.addStringOption('f', "files"), "the added files");
        hashmap.put("files", files);

        CmdLineParser.Option addchannel = parser.addHelp(
                parser.addStringOption('c', "channel"), "add channel or not");
        hashmap.put("channel", addchannel);
        
        CmdLineParser.Option confXML = parser.addHelp(
                parser.addStringOption('x', "xml"), "conf xml file");
        hashmap.put("xml", confXML);

    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        params = new String[5];
        params[0] = (String) parser.getOptionValue(hashmap.get("input"));
        params[1] = (String) parser.getOptionValue(hashmap.get("output"));
        params[2] = (String) parser.getOptionValue(hashmap.get("files"));
        params[3] = (String) parser.getOptionValue(hashmap.get("xml"));
        params[4] = (String) parser.getOptionValue(hashmap.get("channel"));

        if (params[0] == null || params[1] == null || params[2] == null || params[3] == null) {
            throw new Exception("the argument value is null");
        }
        
        if (params[4] == null) {
            params[4] = "0";
        }
    }

    public String[] getDistinctParam() {
        return params;
    }

    public HashMap<String, Option> getHashmap() {
        return hashmap;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
