package com.bi.common.paramparse;
/**
 * 基本的参数解析类 
 * 该类仅仅解析带有基本输入输出的参数，如
 * hadoop jar **.jar -i ****　-ｏ　****
 */

import jargs.gnu.CmdLineParser.Option;

public class BaseCmdParamParse extends AbstractCmdParamParse {

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
        // TODO Auto-generated method stub
        return new Option[0];
    }

}
