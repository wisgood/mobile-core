package com.bi.dingzi.dimensionprocess;

import java.io.File;
import java.io.IOException;

public abstract class AbstractDimProcess<E,T> {

    /** 
     *
     * @Title: main 
     * @Description: 这里用一句话描述这个方法的作用 
     * @param   @param args 参数说明
     * @return void    返回类型说明 
     * @throws 
     */
    
    abstract public void parseDimensionFile(File file) throws IOException;
    abstract public T getDimensionId(E param) throws Exception;

}
