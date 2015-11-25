/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: CommIsLZOArgs.java 
 * @Package com.bi.common.init 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-7-24 下午3:48:19 
 * @input:输入日志路径/2013-7-24
 * @output:输出日志路径/2013-7-24
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.init;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

/**
 * @ClassName: CommIsLZOArgs
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-24 下午3:48:19
 */
public class CommIsLZOArgs implements CLPInterface {

    private String[] commsParam = null;

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

        CmdLineParser.Option inpulzo = parser.addHelp(
                parser.addStringOption('z', "inpulzo"), "is input lzo");
        hashmap.put("inpulzo", inpulzo);

    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);
        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        commsParam = new String[3];
        commsParam[0] = (String) parser.getOptionValue(hashmap.get("input"));
        commsParam[1] = (String) parser.getOptionValue(hashmap.get("output"));
        commsParam[2] = (String) parser.getOptionValue(hashmap.get("inpulzo"));
        if (commsParam[0] == null || commsParam[1] == null
                || commsParam[2] == null) {
            throw new Exception("the argument value is null");
        }
    }

    public String[] getCommsParam() {
        return commsParam;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
