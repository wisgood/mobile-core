package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsplayAfterDistinctMACMR.DistinctPartioner;
import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsplayAfterDistinctMACMR.FsplayAfterDistinctMACCombiner;
import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsplayAfterDistinctMACMR.FsplayAfterDistinctMACMapper;
import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsplayAfterDistinctMACMR.FsplayAfterDistinctMACReducer;

public class FsplayAfterDistinctMAC extends Configured implements Tool {

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

        try {
            fsPlayAfterCalArgs.init("fsplayafterdistinctmac.jar");
            fsPlayAfterCalArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // countArgs.parser.printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(),
                new FsplayAfterDistinctMAC(),
                fsPlayAfterCalArgs.getCountParam());
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub

        Configuration conf = getConf();
        Job job = new Job(conf, "FsplayAfterDistinctMAC");
        job.setJarByClass(FsplayAfterCount.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("delim", args[2]);
        job.setPartitionerClass(DistinctPartioner.class);
        job.setMapperClass(FsplayAfterDistinctMACMapper.class);
        job.setCombinerClass(FsplayAfterDistinctMACCombiner.class);
        job.setReducerClass(FsplayAfterDistinctMACReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
