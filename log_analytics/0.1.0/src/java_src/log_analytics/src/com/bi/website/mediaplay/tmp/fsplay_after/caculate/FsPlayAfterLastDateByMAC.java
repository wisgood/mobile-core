/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlayAfterLastDateByMAC.java 
 * @Package com.bi.website.mediaplay.tmp.fsplay_after.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-21 下午3:08:28 
 */
package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

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

import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsPlayAfterLastDateByMACMR.FsPlayAfterLastDateByMACMapper;
import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsPlayAfterLastDateByMACMR.FsPlayAfterLastDateByMACReduce;

/**
 * @ClassName: FsPlayAfterLastDateByMAC
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-21 下午3:08:28
 */
public class FsPlayAfterLastDateByMAC extends Configured implements Tool {

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
        FsPlayAfterCalArgs fsPlayAfterCalArgs = new FsPlayAfterCalArgs();
        int nRet = 0;
        
        for(int i =0;i<args.length;i++){
            System.out.println(args[i]);
        }
        try {
            fsPlayAfterCalArgs.init("fsplayafterlastdatebymac.jar");
            fsPlayAfterCalArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            // countArgs.parser.printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(),
                new FsPlayAfterLastDateByMAC(),
                fsPlayAfterCalArgs.getCountParam());
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "FsPlayAfterLastDateByMAC");
        job.setJarByClass(FsplayAfterCount.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("delim", args[2]);
        job.setMapperClass(FsPlayAfterLastDateByMACMapper.class);
        job.setReducerClass(FsPlayAfterLastDateByMACReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(60);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
