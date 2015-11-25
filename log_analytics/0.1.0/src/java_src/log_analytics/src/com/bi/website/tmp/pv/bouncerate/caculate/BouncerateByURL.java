/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: BouncerateByURL.java 
 * @Package com.bi.website.tmp.pv.bouncerate.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-24 上午11:18:42 
 */
package com.bi.website.tmp.pv.bouncerate.caculate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.mobile.logs.format.param.LogArgs;
import com.bi.website.tmp.pv.bouncerate.caculate.BouncerateByURLMR.BouncerateByURMapper;
import com.bi.website.tmp.pv.bouncerate.caculate.BouncerateByURLMR.BouncerateByURReducer;

/**
 * @ClassName: BouncerateByURL
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-24 上午11:18:42
 */
public class BouncerateByURL extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "BouncerateByURLMR");
        job.setJarByClass(BouncerateByURLMR.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BouncerateByURMapper.class);
        job.setReducerClass(BouncerateByURReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(4);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    /**
     * @throws Exception
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        LogArgs logArgs = new LogArgs();
        int nRet = 0;
        try {
            logArgs.init("bounceratebyurl.jar");
            logArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            logArgs.getParser().printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new BouncerateByURL(),
                logArgs.getParamStrs());
        System.out.println(nRet);
    }

}
