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

import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsPalyAfterJoinByMACMR.FsPalyAfterJoinByMACMapper;
import com.bi.website.mediaplay.tmp.fsplay_after.caculate.FsPalyAfterJoinByMACMR.FsPalyAfterJoinByMACReducer;

public class FsPalyAfterJoinByMAC extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "FsPlayAfterJoinByMACMRUTL");
        job.setJarByClass(FsPalyAfterJoinByMAC.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FsPalyAfterJoinByMACMapper.class);
        job.setReducerClass(FsPalyAfterJoinByMACReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
         System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        FsPalyAfterJoinByMACArgs countArgs = new FsPalyAfterJoinByMACArgs();
        int nRet = 0;

        try {
            countArgs.init("fsplayafterjoinbymac.jar");
            countArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // countArgs.parser.printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new FsPalyAfterJoinByMAC(),
                countArgs.getCountParam());
        System.out.println(nRet);

    }

}
