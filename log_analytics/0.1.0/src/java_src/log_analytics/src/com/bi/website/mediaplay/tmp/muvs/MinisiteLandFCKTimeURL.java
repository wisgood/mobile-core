/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: MinisiteLandFCKTimeURL.java 
 * @Package com.bi.website.mediaplay.tmp.muvs 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-3 上午11:23:44 
 */
package com.bi.website.mediaplay.tmp.muvs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.bi.comm.util.CommArgs;
import com.bi.website.mediaplay.tmp.muvs.MinisiteLandFCKTimeURLMR.MinisiteLandFCKTimeURLMapper;
import com.bi.website.mediaplay.tmp.muvs.MinisiteLandFCKTimeURLMR.MinisiteLandFCKTimeURLReducer;

/**
 * @ClassName: MinisiteLandFCKTimeURL
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-3 上午11:23:44
 */
public class MinisiteLandFCKTimeURL extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "MinisiteLandFCKTimeURL");
        job.setJarByClass(MinisiteLandFCKTimeURL.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MinisiteLandFCKTimeURLMapper.class);
        job.setReducerClass(MinisiteLandFCKTimeURLReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(60);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        CommArgs countArgs = new CommArgs();
        int nRet = 0;

        try {
            countArgs.init("minisitelandfcktimeurl.jar");
            countArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // countArgs.parser.printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(),
                new MinisiteLandFCKTimeURL(), countArgs.getCommsParam());
        System.out.println(nRet);

    }
}
