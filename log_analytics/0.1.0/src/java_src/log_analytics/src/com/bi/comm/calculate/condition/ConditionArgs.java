/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: ConditionArgs.java 
 * @Package com.bi.comm.calculate.condition 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-30 下午2:47:11 
 */
package com.bi.comm.calculate.condition;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

/**
 * @ClassName: ConditionArgs
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-30 下午2:47:11
 */
public class ConditionArgs implements CLPInterface {

    private String[] conditionParam = null;

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

        conditionParam = new String[5];
        conditionParam[0] = (String) parser
                .getOptionValue(hashmap.get("input"));
        conditionParam[1] = (String) parser.getOptionValue(hashmap
                .get("output"));
        conditionParam[2] = (String) parser.getOptionValue(hashmap
                .get("groupby"));
        conditionParam[3] = (String) parser.getOptionValue(hashmap
                .get("conditioncol"));
        conditionParam[4] = (String) parser.getOptionValue(hashmap
                .get("conditionvalue"));
        if (conditionParam[0] == null || conditionParam[1] == null
                || conditionParam[2] == null || conditionParam[3] == null
                || conditionParam[4] == null) {
            throw new Exception("the argument value is null");
        }
        if (conditionParam[3].split(",").length != conditionParam[4]
                        .split(",").length) {
            throw new Exception(
                    "the condition index count does not match condition value count!");

        }
    }

    public String[] getConditionParam() {
        return conditionParam;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
