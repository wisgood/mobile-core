package com.bi.common.etl;

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

import com.bi.common.etl.InfohashLogFormatMR.InfohashLogFormatMapper;
import com.bi.common.etl.InfohashLogFormatMR.InfohashLogFormatReducer;
import com.bi.common.etl.LogFormatMR.LogFormatMapper;
import com.bi.common.etl.LogFormatMR.LogFormatReducer;
import com.bi.common.etl.util.HdfsUtil;

public class LogFormat extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("files", args[2]);
        conf.set("format_xml_file", args[3]);
        conf.set("channel_type", args[4]);
        
        Job job = new Job(conf, "LogFormat");
        job.setJarByClass(LogFormat.class);
        HdfsUtil.deleteDir(args[1]);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        if("2".equalsIgnoreCase(args[4]) || "1".equalsIgnoreCase(args[4])){
            job.setMapperClass(InfohashLogFormatMapper.class);
            job.setReducerClass(InfohashLogFormatReducer.class);
        }else{
            job.setMapperClass(LogFormatMapper.class);
            job.setReducerClass(LogFormatReducer.class);
        }
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(4);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        LogFormatArgs logFormatArgs = new LogFormatArgs();
        int nRet = 0;

        try {
            logFormatArgs.init("LogFormat.jar");
            logFormatArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new LogFormat(),
                logFormatArgs.getDistinctParam());
        System.out.println(nRet);

    }
}
