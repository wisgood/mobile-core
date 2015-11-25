/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: UserLostList.java 
 * @Package com.bi.dingzi.day.user 
 * @Description: 用一句话描述该文件做什么
 * @author niewf
 * @date 2013-6-4 下午5:08:57 
 */
package com.bi.comm.calculate.distinct.set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.calculate.distinct.set.OperationMR.OperationMap;
import com.bi.comm.calculate.distinct.set.OperationMR.OperationReduce;
import com.hadoop.mapreduce.LzoTextInputFormat;


/**
 * 
* @ClassName: UserLostList 
* @Description: 这里用一句话描述这个类的作用 
* @author niewf 
* @date 2013-8-7 上午2:44:40
 */
public class Operation extends Configured implements Tool {

    /**
     * 
    *
    * @Title: main 
    * @Description: 这里用一句话描述这个方法的作用 
    * @param   @param args
    * @param   @throws Exception 参数说明
    * @return void    返回类型说明 
    * @throws
     */

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        OperationArgs operationArgsArgs = new OperationArgs();
        int nRet = 0;
        try {
            operationArgsArgs.init("Operation.jar");

            operationArgsArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            // countArgs.parser.printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new Operation(),
                operationArgsArgs.getCountParam());
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "tmp_job_setoperation");
        job.setJarByClass(Operation.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("left", args[2]);
        job.getConfiguration().set("column", args[3]);
        job.getConfiguration().set("operation", args[4]);
        job.getConfiguration().set("separator", args[5]);
		if ((args.length == 7) && (args[6].compareToIgnoreCase("lzo") == 0)) {
			job.setInputFormatClass(LzoTextInputFormat.class);
		}
        job.setMapperClass(OperationMap.class);
        job.setReducerClass(OperationReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(60);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
