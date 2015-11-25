package com.bi.ibidian.jargsparser;

import jargs.gnu.CmdLineParser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//import org.apache.log4j.Logger;

public class AutoHelpParser extends CmdLineParser
{
	//private static Logger logger = Logger.getLogger(AutoHelpParser.class.getName());
	private String funcNameMsg = null;
	private String funcUsageMsg = null;
	private List<String> optionHelpMsgs = new ArrayList<String>();

	public Option addOptionHelpMsgs(Option option, String helpMsg)
	{
		this.optionHelpMsgs.add("-" + option.shortForm() + "/--" + option.longForm() + ": "
				+ helpMsg);
		return option;
	}

	public void setFuncNameMsg(String funcNameMsg)
	{
		this.funcNameMsg = "Function: " + funcNameMsg + "\n";
	}

	public void setFuncUsageMsg(String funcUsageMsg)
	{
		this.funcUsageMsg = "Usage:    " + funcUsageMsg + "\n";
	}

	public void printFuncUsage()
	{
		System.err.println(funcNameMsg + funcUsageMsg + "Args Description:\n");
		for (Iterator<String> i = optionHelpMsgs.iterator(); i.hasNext();)
		{
			//logger.error("    " + i.next());
			System.err.println("\t" + i.next());
		}
	}

}
