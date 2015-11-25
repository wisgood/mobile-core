package com.bi.common.paramparse;

/**
 * 基本的参数解析类 
 * 该类仅仅解析带有基本输入输出和配置文件的参数，如
 * hadoop jar **.jar -i ****　-ｏ　**** --files 
 * 因为--files参数文件比较特殊，需要作特殊处理
 */

import jargs.gnu.CmdLineParser.Option;

import java.util.ArrayList;
import java.util.List;

public class ConfigFileCmdParamParse extends AbstractCmdParamParse {

    @Override
    public String getFunctionDescription() {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getFunctionUsage() {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public Option[] getOptions() {
        List<Option> options = new ArrayList<Option>(0);
        Option option = getParser().addHelp(
                getParser().addStringOption("files"),
                "confige file to resove dimention");
        options.add(option);
        return options.toArray(new Option[options.size()]);
    }

    @Override
    public String[] getParams() {
        // TODO Auto-generated method stub

        String[] params = super.getParams();
        List<String> list = new ArrayList<String>();
        list.add("-files");
        list.add(params[2]);
        list.add(params[0]);
        list.add(params[1]);
        return list.toArray(new String[list.size()]);
    }

}
