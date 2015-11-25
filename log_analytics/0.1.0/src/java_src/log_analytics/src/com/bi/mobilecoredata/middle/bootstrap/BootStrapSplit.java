package com.bi.mobilecoredata.middle.bootstrap;

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

import com.bi.mobilecoredata.middle.bootstrap.BootStrapSplitMR.BootStrapSplitMapper;
import com.bi.mobilecoredata.middle.bootstrap.BootStrapSplitMR.BootStrapSplitReducer;


public class BootStrapSplit extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub

        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(BootStrapSplit.class);
        job.setJobName("BootStrapSplit");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BootStrapSplitMapper.class);
        job.setReducerClass(BootStrapSplitReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String... args) throws Exception {
        BootStrapConditionArgs bootStrapConditionArgs = new BootStrapConditionArgs();
        int nRet = 0;

        try {
            bootStrapConditionArgs.init("bootstrapsplitjar");
            bootStrapConditionArgs.parse(args);
        }
        catch(Exception e) {
            System.exit(1);

        }

        nRet = ToolRunner.run(new Configuration(),
                new BootStrapSplit(),
                bootStrapConditionArgs.getParams());
        System.out.println(nRet);

    }

}
