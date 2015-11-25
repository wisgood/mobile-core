/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PushReachMessTypeConditionArgs.java 
 * @Package com.bi.mobilecoredata.middle.pushreach.messtype.conditions 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-7 下午4:47:21 
 */
package com.bi.mobilecoredata.middle.pushreach.messtype.condition;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

/**
 * @ClassName: PushReachMessTypeConditionArgs
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-7 下午4:47:21
 */
public class PushReachMessTypeConditionArgs implements CLPInterface {

    private String[] conditionssParam = null;

    private HashMap<String, Option> hashmap = null;

    private AutoHelpParser parser = null;

    @Override
    public void init(String jarName) throws Exception {
        // TODO Auto-generated method stub
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

        CmdLineParser.Option conditions = parser.addHelp(
                parser.addStringOption('c', "conditions"),
                "the given conditions,e.g 1,2,3");
        hashmap.put("conditions", conditions);
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

        conditionssParam = new String[3];
        conditionssParam[0] = (String) parser.getOptionValue(hashmap
                .get("input"));
        conditionssParam[1] = (String) parser.getOptionValue(hashmap
                .get("output"));
        conditionssParam[2] = (String) parser.getOptionValue(hashmap
                .get("conditions"));
        if (conditionssParam[0] == null || conditionssParam[1] == null
                || conditionssParam[2] == null) {
            throw new Exception("the argument value is null");
        }
    }

    public String[] getConditionssParam() {
        return conditionssParam;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
