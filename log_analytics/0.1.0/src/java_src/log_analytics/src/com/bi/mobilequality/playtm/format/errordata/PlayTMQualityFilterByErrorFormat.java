/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayTMQualityFilterByErrorFormat.java 
 * @Package com.bi.mobilequality.playtm.format.correctdata 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-10 下午2:37:34 
 */
package com.bi.mobilequality.playtm.format.errordata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.mobile.logs.format.param.LogArgs;
import com.bi.mobilequality.playtm.format.errordata.PlayTMQualityFilterByErrorFormatMR.PlayTMQualityFilterByErrorFormatMap;
import com.bi.mobilequality.playtm.format.errordata.PlayTMQualityFilterByErrorFormatMR.PlayTMQualityFilterByErrorFormatReduce;

/**
 * @ClassName: PlayTMQualityFilterByErrorFormat
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-10 下午2:37:34
 */
public class PlayTMQualityFilterByErrorFormat extends Configured implements
        Tool {

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "PlayTMQualityFilterByErrorFormatMR");
        job.setJarByClass(PlayTMQualityFilterByErrorFormatMR.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PlayTMQualityFilterByErrorFormatMap.class);
        job.setReducerClass(PlayTMQualityFilterByErrorFormatReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        LogArgs logArgs = new LogArgs();
        int nRet = 0;
        try {
            logArgs.init("playtmqualityformaterror.jar");
            logArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            logArgs.getParser().printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(),
                new PlayTMQualityFilterByErrorFormat(), logArgs.getParamStrs());
        System.out.println(nRet);
    }
}
