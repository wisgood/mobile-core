/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlayAfterCalArgs.java 
 * @Package com.bi.website.mediaplay.tmp.fsplay_after.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-21 下午2:51:21 
 */
package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

/**
 * @ClassName: FsPlayAfterCalArgs
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-21 下午2:51:21
 */
public class FsPlayAfterCalArgs implements CLPInterface {

    private String[] fsPlayAfterParam = null;

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

        CmdLineParser.Option delim = parser.addHelp(
                parser.addStringOption('d', "delim"), "the given delim,e.g \t");
        hashmap.put("delim", delim);

    }

    public void parse(String[] args) throws Exception {

        parser.parse(args);

        if (args.length == 0
                || Boolean.TRUE.equals(parser.getOptionValue(hashmap
                        .get("help")))) {
            throw new Exception("No args or --help");
        }

        fsPlayAfterParam = new String[3];
        fsPlayAfterParam[0] = (String) parser.getOptionValue(hashmap
                .get("input"));
        fsPlayAfterParam[1] = (String) parser.getOptionValue(hashmap
                .get("output"));
        fsPlayAfterParam[2] = (String) parser.getOptionValue(hashmap
                .get("delim"));
        if (fsPlayAfterParam[2].equalsIgnoreCase("a")) {
            fsPlayAfterParam[2] = "\t";
        }
        if (fsPlayAfterParam[2].equalsIgnoreCase("b")) {
            fsPlayAfterParam[2] = ",";
        }
        if (fsPlayAfterParam[0] == null || fsPlayAfterParam[1] == null
                || fsPlayAfterParam[2] == null) {
            throw new Exception("the argument value is null");
        }
    }

    public String[] getCountParam() {
        return fsPlayAfterParam;
    }

    public AutoHelpParser getParser() {
        return parser;
    }

}
