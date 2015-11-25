/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlayAfterAddIpIsChangeList.java 
 * @Package com.bi.website.mediaplay.tmp.fsplay_after.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-6 上午11:31:41 
 */
package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

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
import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsPlayAfterAddIpIsChangeListMR.FsPlayAfterAddIpIsChangeListMapper;
import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsPlayAfterAddIpIsChangeListMR.FsPlayAfterAddIpIsChangeListReducer;

/**
 * @ClassName: FsPlayAfterAddIpIsChangeList
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-6 上午11:31:41
 */
public class FsPlayAfterAddIpIsChangeList extends Configured implements Tool {

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
            logArgs.init("fsplayafteraddipischangelist.jar");

            logArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            // countArgs.parser.printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(),
                new FsPlayAfterAddIpIsChangeList(), logArgs.getParamStrs());
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "FsPlayAfterAddIpIsChangeList");
        job.setJarByClass(FsPlayAfterAddIpIsChangeListMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FsPlayAfterAddIpIsChangeListMapper.class);
        job.setReducerClass(FsPlayAfterAddIpIsChangeListReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(60);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
