/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: PushReachOkCondition.java 
* @Package com.bi.mobilecoredata.middle.pushreach.ok.condition 
* @Description: 用一句话描述该文件做什么
* @author fuys
* @date 2013-5-7 下午5:34:43 
*/
package com.bi.mobilecoredata.middle.pushreach.ok.condition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.mobilecoredata.middle.pushreach.ok.condition.PushReachOkConditionMR.PushReachOkConditionMapper;
import com.bi.mobilecoredata.middle.pushreach.ok.condition.PushReachOkConditionMR.PushReachOkConditionReduce;



/** 
 * @ClassName: PushReachOkCondition 
 * @Description: 这里用一句话描述这个类的作用 
 * @author fuys 
 * @date 2013-5-7 下午5:34:43  
 */
public class PushReachOkCondition extends Configured implements Tool {

    /**
     * @throws Exception  
     *
     * @Title: main 
     * @Description: 这里用一句话描述这个方法的作用 
     * @param   @param args 参数说明
     * @return void    返回类型说明 
     * @throws 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        PushReachOkConditionArgs pushReachOkConditionArgs = new PushReachOkConditionArgs();
        int nRet = 0;
        
  
        try {
            pushReachOkConditionArgs
                    .init("pushreachokcondition.jar");
            pushReachOkConditionArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // countArgs.parser.printUsage();
           // e.printStackTrace();
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(),
                new PushReachOkCondition(),
                pushReachOkConditionArgs.getOkParam());
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "PushReachOkConditionMR");
        job.setJarByClass(PushReachOkConditionMR.class);
        job.getConfiguration().set("ok", args[2]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PushReachOkConditionMapper.class);
        job.setReducerClass(PushReachOkConditionReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
