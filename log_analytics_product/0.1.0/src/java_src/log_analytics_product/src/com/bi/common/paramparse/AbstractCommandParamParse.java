package com.bi.common.paramparse;


import jargs.gnu.CmdLineParser.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.bi.common.init.AutoHelpParser;


public abstract class AbstractCommandParamParse {

    private String[] params = null;

    private List<Option> optionList = null;

    private AutoHelpParser parser = null;

    public AbstractCommandParamParse() {
        parser = new AutoHelpParser();
        optionList = new ArrayList<Option>();
        init();
    }

    public void init() {

        addFuntionDescription(getFunctionDescription());
        addFunctionUsage(getFunctionUsage());
//        Option help = parser.addHelp(parser.addBooleanOption('h', "help"),
//                "return the help doc");

        Option input = parser.addHelp(parser.addStringOption('i', "input"),
                "the input file or path");

        Option output = parser
                .addHelp(parser.addStringOption('o', "output"),
                        "the hadoop result output path,the path does not in the cluster");

//        optionList.add(help);
        optionList.add(input);
        optionList.add(output);
        for (Option option : getOptions()) {
            optionList.add(option);
        }

    }

    public void parse(String args[]) throws Exception {

        if (args == null || args.length == 0) {
            throw new Exception("No args");
        }

        parser.parse(args);
        params = new String[optionList.size()];
        Iterator<Option> iterator = optionList.iterator();
        int counter = 0;
        while (iterator.hasNext()) {
            Option option = (Option) iterator.next();
            String value = (String) parser.getOptionValue(option);
            if (null == value) {
                parser.printUsage();
                throw new Exception("the argument" + option.longForm()
                        + " value is null");
            }
            params[counter++] = value;

        }

    }

    public String[] getParams() {
        return params;
    }

    public void addFuntionDescription(String functionDescription) {
        parser.addFunction(functionDescription);
    }

    public void addFunctionUsage(String functionUsage) {
        parser.addUsage(functionUsage);
    }

    public abstract String getFunctionDescription();

    public abstract String getFunctionUsage();

    public abstract Option[] getOptions();

    public AutoHelpParser getParser() {
        return parser;
    }
    

}
