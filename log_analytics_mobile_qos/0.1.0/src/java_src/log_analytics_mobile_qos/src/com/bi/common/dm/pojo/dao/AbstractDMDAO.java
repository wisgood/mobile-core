package com.bi.common.dm.pojo.dao;


import java.io.File;
import java.io.IOException;
/**
 * 
 * @author fysyihui
 *
 * @param <E> function parameters
 * @param <T> function return types
 */
public abstract class AbstractDMDAO<E,T> {

	abstract public void parseDMObj(File file) throws IOException;
	
	abstract public T getDMOjb(E param) throws Exception;
	
	protected boolean isContainsEmptyStrs(String[] strArgs) {
		for(int i=0;i<strArgs.length;i++){
			if ("".equalsIgnoreCase(strArgs[i])) {
				return true;
			}

		}
		return false;
	}
}
