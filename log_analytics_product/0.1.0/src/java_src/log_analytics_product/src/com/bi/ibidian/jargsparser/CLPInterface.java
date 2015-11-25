package com.bi.ibidian.jargsparser;

/**
 * 
 * @author fuys
 * 
 */
public interface CLPInterface
{
	public void initParserOptions(String jarName) throws Exception;

	public void parseArgs(String[] args) throws Exception;
}
