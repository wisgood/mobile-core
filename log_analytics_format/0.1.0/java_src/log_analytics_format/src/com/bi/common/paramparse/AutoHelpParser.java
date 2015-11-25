package com.bi.common.paramparse;


import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;


public class AutoHelpParser extends CmdLineParser {
    private static Logger logger = Logger.getLogger(AutoHelpParser.class.getName());
    private String function = null;
    private String usage    = null;
    private List<String> optionHelpStrings  = new ArrayList<String>();
    

    public Option addHelp(Option option, String helpString) {
        optionHelpStrings.add(" -" + option.shortForm() + "/--" + option.longForm() + ": " + helpString);
        return option;
    }
    
    public void addFunction(String func){
        function = " Function :  \n    " + func +"\n";
    }

    public void addUsage(String usageValue){
        usage = " Usage :  \n    " + usageValue + "\n";
    }    
    
    public void printUsage() {
        System.err.print(function + usage + " Args Description:\n");
        for (Iterator<String> i = optionHelpStrings.iterator(); i.hasNext(); ) {
            logger.error("    " + i.next());
        }
    }           
}
