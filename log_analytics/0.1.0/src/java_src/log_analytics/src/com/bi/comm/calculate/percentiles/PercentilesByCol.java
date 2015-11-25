/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PercentilesByCol.java 
 * @Package com.bi.comm.calculate.percentiles 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-14 下午1:43:40 
 */
package com.bi.comm.calculate.percentiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.calculate.percentiles.PercentilesByColMRUTL.PercentilesByColMapper;
import com.bi.comm.calculate.percentiles.PercentilesByColMRUTL.PercentilesByColReducer;

/**
 * @ClassName: PercentilesByCol
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-14 下午1:43:40
 */
public class PercentilesByCol extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "PercentilesByColMRUTL");
        job.setJarByClass(PercentilesByCol.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("groupby", args[2]);
        job.getConfiguration().set("percent", args[3]);
        job.getConfiguration().set("pncolindex", args[4]);
        job.setMapperClass(PercentilesByColMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(PercentilesByColReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        PercentilesByColArgs percentitlesArgs = new PercentilesByColArgs();
        int nRet = 0;

        try {
            percentitlesArgs.init("percentitlesbycol.jar");
            percentitlesArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new PercentilesByCol(),
                percentitlesArgs.getPercentilesParam());
        System.out.println(nRet);

    }

}
