/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PercentilesByColConditionCondition.java 
 * @Package com.bi.comm.calculate.percentiles.condition 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-15 下午6:20:29 
 */
package com.bi.comm.calculate.percentiles.condition;

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

import com.bi.comm.calculate.percentiles.condition.PercentilesByColConditionMRUTL.PercentilesByColConditionMapper;
import com.bi.comm.calculate.percentiles.condition.PercentilesByColConditionMRUTL.PercentilesByColConditionReducer;

/**
 * @ClassName: PercentilesByColConditionCondition
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-15 下午6:20:29
 */
public class PercentilesByColCondition extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "PercentilesByColConditionMRUTL");
        job.setJarByClass(PercentilesByColCondition.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("groupby", args[2]);
        job.getConfiguration().set("percent", args[3]);
        job.getConfiguration().set("pncolindex", args[4]);
        job.getConfiguration().set("conditioncol", args[5]);
        job.getConfiguration().set("conditionvalue", args[6]);
        job.setMapperClass(PercentilesByColConditionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(PercentilesByColConditionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        PercentilesByColConditionArgs percentitlesConditionArgs = new PercentilesByColConditionArgs();
        int nRet = 0;

        try {
            percentitlesConditionArgs.init("percentitlesbycolcondition.jar");
            percentitlesConditionArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(),
                new PercentilesByColCondition(),
                percentitlesConditionArgs.getPercentilesParam());
        System.out.println(nRet);

    }
}
