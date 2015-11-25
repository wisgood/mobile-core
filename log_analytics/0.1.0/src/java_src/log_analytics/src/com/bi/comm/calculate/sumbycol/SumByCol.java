package com.bi.comm.calculate.sumbycol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.calculate.sumbycol.SumByColMRUTL.SumByColMap;
import com.bi.comm.calculate.sumbycol.SumByColMRUTL.SumByColReduce;
import com.bi.mobile.comm.constant.ConstantEnum;

public class SumByCol extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.set(ConstantEnum.COLUMNSUMKEY.name(), args[2]);
        // System.out.println("*********************************************");
        // System.out.println("colum:"+args[2]);
        // System.out.println("*********************************************");
        conf.set(ConstantEnum.SUMCOL.name(), args[3]);
        // conf.set("mapred.job.tracker", "cluster-01:18001");

        Job job = new Job(conf, "SumByColMRUTL");
        job.setJarByClass(SumByCol.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SumByColMap.class);
        job.setCombinerClass(SumByColReduce.class);
        job.setReducerClass(SumByColReduce.class);

        job.setMapperClass(SumByColMap.class);
        job.setReducerClass(SumByColReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(60);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        SumColArgs sumArgs = new SumColArgs();
        int nRet = 0;

        try {
            sumArgs.init("sumbycol.jar");
            sumArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // sumArgs.parser.printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new SumByCol(),
                sumArgs.getSumParam());
        System.out.println(nRet);

    }
}
