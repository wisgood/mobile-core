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

import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsPalyAfterJoinByMACArgs;
import com.bi.website.mediaplay.tmp.muvs.PVDataMuvsJoinOtherMR.PVDataMuvsJoinOtherMapper;
import com.bi.website.mediaplay.tmp.muvs.PVDataMuvsJoinOtherMR.PVDataMuvsJoinOtherReducer;


public class PVDataMuvsJoinOther extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "PVDataMuvsJoinOther");
        job.setJarByClass(PVDataMuvsJoinOther.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PVDataMuvsJoinOtherMapper.class);
        job.setReducerClass(PVDataMuvsJoinOtherReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(60);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        FsPalyAfterJoinByMACArgs countArgs = new FsPalyAfterJoinByMACArgs();
        int nRet = 0;

        try {
            countArgs.init("pvdatamuvsjoinother.jar");
            countArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // countArgs.parser.printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new PVDataMuvsJoinOther(),
                countArgs.getCountParam());
        System.out.println(nRet);

    }

}
