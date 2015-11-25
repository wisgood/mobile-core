/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: IOSWatchDownRateMR.java 
* @Package com.bi.calculate 
* @Description: 对日志名进行处理
* @author fuys
* @date 2013-9-29 下午6:24:55 
* @input:输入日志路径/2013-9-29
* @output:输出日志路径/2013-9-29
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.calculate;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/** 
 * @ClassName: IOSWatchDownRateMR 
 * @Description: 这里用一句话描述这个类的作用 
 * @author fuys 
 * @date 2013-9-29 下午6:24:55  
 */
public class IOSWatchDownRateMR extends Configured implements Tool {

    /** 
     *
     * @Title: main 
     * @Description: 这里用一句话描述这个方法的作用 
     * @param   @param args 参数说明
     * @return void    返回类型说明 
     * @throws 
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

    /** (非 Javadoc) 
    * <p>Title: run</p> 
    * <p>Description: </p> 
    * @param args
    * @return
    * @throws Exception 
    * @see org.apache.hadoop.util.Tool#run(java.lang.String[]) 
    */
    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

}
