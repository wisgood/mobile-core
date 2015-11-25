package com.bi.ibidian.dimension;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.bi.ibidian.datadefine.CommonEnum;

/**
 * @param <E>
 *            function parameters
 * @param <T>
 *            function return types
 */
public abstract class AbstractDimDAO
{
	abstract public void parseDimFile(File file) throws IOException;

	abstract public Map<CommonEnum, String> getDimTransMap(Object paramKey) throws Exception;

	protected boolean isContainEmptyStrs(String[] strArgs)
	{
		for (int i = 0; i < strArgs.length; i++)
		{
			if ("".equalsIgnoreCase(strArgs[i]))
			{
				return true;
			}
		}
		return false;
	}

}
