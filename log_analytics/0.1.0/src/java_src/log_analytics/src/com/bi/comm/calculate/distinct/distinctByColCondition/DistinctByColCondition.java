/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DistinctByColCondition.java 
 * @Package com.bi.comm.calculate.percentiles.condition 
 * @Description: 用一句话描述该文件做什么
 * @author niewf
 * @date 2013-5-15 下午6:20:29 
 */
package com.bi.comm.calculate.distinct.distinctByColCondition;

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

import com.bi.comm.calculate.distinct.distinctByColCondition.DistinctByColConditionMRUTL.DistinctByColConditionMapper;
import com.bi.comm.calculate.distinct.distinctByColCondition.DistinctByColConditionMRUTL.DistinctByColConditionReducer;
import com.hadoop.mapreduce.LzoTextInputFormat;


/**
 * 
* @ClassName: DistinctByColCondition 
* @Description: 这里用一句话描述这个类的作用 
* @author niewf 
* @date 2013-8-11 下午7:48:17
 */
 
public class DistinctByColCondition extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "DistinctByColConditionMRUTL");
        job.setJarByClass(DistinctByColCondition.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("distinctcolindex", args[2]);
        job.getConfiguration().set("conditioncol", args[3]);
        job.getConfiguration().set("conditionvalue", args[4]);
        job.getConfiguration().set("separator", args[5]);
		if (args[6].compareToIgnoreCase("lzo") == 0) {
			job.setInputFormatClass(LzoTextInputFormat.class);
		}

        job.setMapperClass(DistinctByColConditionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(DistinctByColConditionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        DistinctByColConditionArgs distinctByColConditionArgs = new DistinctByColConditionArgs();
        int nRet = 0;

        try {
            distinctByColConditionArgs.init("distinctbycolcondition.jar");
            distinctByColConditionArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(),
                new DistinctByColCondition(),
                distinctByColConditionArgs.getParams());
        System.out.println(nRet);

    }
}
