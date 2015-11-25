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
        int length = params.length;
        // file param must be the first
        String[] convertParam = new String[length + 1];
        System.arraycopy(params, 0, convertParam, 2, length - 1);
        convertParam[0] = "-files";
        convertParam[1] = params[length - 1];
        return convertParam;
    }

    public static void main(String[] args) {
        String[] params = { "aa", "bb" };
        int length = params.length;
        // file param must be the first
        String[] convertParam = new String[length + 1];
        System.arraycopy(params, 0, convertParam, 2, length - 1);
        convertParam[0] = "-files";
        convertParam[1] = params[length - 1];
    }

}
