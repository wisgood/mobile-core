/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekInfoArgs.java 
 * @Package com.bi.dingzi.day.user 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-7-20 下午4:58:21 
 * @input:输入日志路径/2013-7-20
 * @output:输出日志路径/2013-7-20
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.dingzi.day.user;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.common.init.AutoHelpParser;
import com.bi.common.init.CLPInterface;

/**
 * @ClassName: WeekInfoArgs
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-20 下午4:58:21
 */
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
