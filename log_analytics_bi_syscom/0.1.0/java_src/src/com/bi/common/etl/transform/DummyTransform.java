package com.bi.common.etl.transform;

/**
 * 
* @ClassName: DummyTransform 
* @Description: 对此字段什么也不做 
* @author niewf
* @date Oct 23, 2013 12:18:00 AM
 */
public class DummyTransform implements Transform {

    @Override
    public String process(String origin, String sperator) {
        // TODO Auto-generated method stub
        return origin;
    }
}
