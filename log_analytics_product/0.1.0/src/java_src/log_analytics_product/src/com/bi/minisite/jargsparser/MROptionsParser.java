package com.bi.minisite.jargsparser;

import jargs.gnu.CmdLineParser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MROptionsParser extends CmdLineParser
{
	private String jarName = null;

	private List<String> optionsDescription = new ArrayList<String>();

	public void setJarName(String jarName)
	{
		this.jarName = jarName;
	}

	public Option setOptionDescription(Option option, String description)
	{
		this.optionsDescription.add("-" + option.shortForm() + "/--" + option.longForm() + ": "
				+ description);
		return option;
	}

	public void printMRJarUsage()
	{
		System.err.println("Jar: " + jarName);
		System.err.println("Usage: hadoop jar " + jarName + "main-class-path <option> <value> ...");
		System.err.println("Options Description:\n");
		for (Iterator<String> i = optionsDescription.iterator(); i.hasNext();)
		{
			System.err.println("\t" + i.next());
		}
	}

}
