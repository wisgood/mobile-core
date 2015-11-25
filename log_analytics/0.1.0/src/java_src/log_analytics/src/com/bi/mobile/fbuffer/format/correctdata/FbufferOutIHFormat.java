package com.bi.mobile.fbuffer.format.correctdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.mobile.fbuffer.format.correctdata.FbufferOutIHFormatMR.FbufferOutIHFormatMap;
import com.bi.mobile.fbuffer.format.correctdata.FbufferOutIHFormatMR.FbufferOutIHFormatReduce;
import com.bi.mobile.logs.format.param.LogArgs;

public class FbufferOutIHFormat extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "FbufferOutIHFormatMR");
        job.setJarByClass(FbufferOutIHFormatMR.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FbufferOutIHFormatMap.class);
        job.setReducerClass(FbufferOutIHFormatReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
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
            logArgs.init("fbufferotheretl.jar");
            logArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            logArgs.getParser().printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new FbufferOutIHFormat(),
                logArgs.getParamStrs());
        System.out.println(nRet);
    }
}
