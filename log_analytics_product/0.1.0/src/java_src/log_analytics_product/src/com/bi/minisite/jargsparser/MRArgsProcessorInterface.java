package com.bi.minisite.jargsparser;

public interface MRArgsProcessorInterface
{
	public void initDefaultOptions(String jarName) throws Exception;

	public void parseAndCheckArgs(String[] args) throws Exception;
}
