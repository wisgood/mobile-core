/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: TestDataFormatUtils.java 
 * @Package com.bi.common.util 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-22 下午3:46:56 
 * @input:输入日志路径/2013-8-22
 * @output:输出日志路径/2013-8-22
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.util;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bi.common.constant.CommonConstant;

/**
 * @ClassName: TestDataFormatUtils
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-22 下午3:46:56
 */
public class TestDataFormatUtils {

    /**
     * 
     * @Title: setUp
     * @Description: 这里用一句话描述这个方法的作用
     * @param @throws java.lang.Exception 参数说明
     * @return void 返回类型说明
     * @throws
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * 
     * @Title: tearDown
     * @Description: 这里用一句话描述这个方法的作用
     * @param @throws java.lang.Exception 参数说明
     * @return void 返回类型说明
     * @throws
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for
     * {@link com.bi.common.util.DataFormatUtils#split(java.lang.String, char, int)}
     * .
     */
    @Test
    public void testSplit() {
        fail("Not yet implemented");
    }

    @Test
    public void testSubStr() {

        String timeStrError = "20130814023706";
        // String timeStrInfo = "";

        System.out.println(timeStrError.substring(0,
                CommonConstant.DATE_FORMAT.length()));

    }
    
    @Test
    public void testLongToIp(){
        
        
        System.out.println(DataFormatUtils.longToIP(16843013));
    }

}
