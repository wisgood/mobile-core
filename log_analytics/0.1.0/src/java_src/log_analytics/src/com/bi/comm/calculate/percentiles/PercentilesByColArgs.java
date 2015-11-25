/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PercentilesByColArgs.java 
 * @Package com.bi.comm.calculate.percentiles 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-14 下午1:23:42 
 */
package com.bi.comm.calculate.percentiles;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

/**
 * @ClassName: PercentilesByColArgs
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-14 下午1:23:42
 */
public class PercentilesByColArgs implements CLPInterface {

    private String[] percentilesParam = null;

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
    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        percentilesParam = new String[5];
        percentilesParam[0] = (String) parser.getOptionValue(hashmap
                .get("input"));
        percentilesParam[1] = (String) parser.getOptionValue(hashmap
                .get("output"));
        percentilesParam[2] = (String) parser.getOptionValue(hashmap
                .get("groupby"));
        percentilesParam[3] = (String) parser.getOptionValue(hashmap
                .get("percent"));
        percentilesParam[4] = (String) parser.getOptionValue(hashmap
                .get("pncolindex"));
        if (percentilesParam[0] == null || percentilesParam[1] == null
                || percentilesParam[2] == null || percentilesParam[3] == null
                || percentilesParam[4] == null) {
            throw new Exception("the argument value is null");
        }
    }

    public AutoHelpParser getParser() {
        return parser;
    }

    public String[] getPercentilesParam() {
        return percentilesParam;
    }

}
