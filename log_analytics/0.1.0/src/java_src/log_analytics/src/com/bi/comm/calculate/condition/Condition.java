/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: Condition.java 
 * @Package com.bi.comm.calculate.condition 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-30 下午3:03:42 
 */
package com.bi.comm.calculate.condition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.calculate.condition.ConditionMRUTL.ConditionMRUTLMapper;
import com.bi.comm.calculate.condition.ConditionMRUTL.ConditionMRUTLReducer;

/**
 * @ClassName: Condition
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-30 下午3:03:42
 */
public class Condition extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "ConditionByColMRUTL");
        job.setJarByClass(Condition.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("groupby", args[2]);
        job.getConfiguration().set("conditioncol", args[3]);
        job.getConfiguration().set("conditionvalue", args[4]);
        job.setMapperClass(ConditionMRUTLMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ConditionMRUTLReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
    public static void main(String[] args) throws Exception {

        ConditionArgs conditionArgs = new ConditionArgs();
        int nRet = 0;

        try {
            conditionArgs.init("conditionbycol.jar");
            conditionArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(), new Condition(),
                conditionArgs.getConditionParam());
        System.out.println(nRet);

    }

}
