/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: CountColMRUTL.java 
* @Package com.bi.common.cal 
* @Description: 对日志名进行处理
* @author fuys
* @date 2013-7-31 下午2:41:06 
* @input:输入日志路径/2013-7-31
* @output:输出日志路径/2013-7-31
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.common.cal;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/** 
 * @ClassName: CountColMRUTL 
 * @Description: 这里用一句话描述这个类的作用 
 * @author fuys 
 * @date 2013-7-31 下午2:41:06  
 */
public class CountColMRUTL extends Configured implements Tool{

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
