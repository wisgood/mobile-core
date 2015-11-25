package com.bi.comm.calculate.distinct;

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

import com.bi.comm.calculate.distinct.DistinctByColMRUTL.DistinctByColCombiner;
import com.bi.comm.calculate.distinct.DistinctByColMRUTL.DistinctByColMapper;
import com.bi.comm.calculate.distinct.DistinctByColMRUTL.DistinctByColReduce;
import com.bi.comm.calculate.distinct.DistinctByColMRUTL.DistinctPartioner;

public class DistinctByCol extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("column", args[2]);
        conf.set("distbycolum", args[3]);
        conf.set("delim", args[4]);
        Job job = new Job(conf, "DistinctByColMRUTL");
        job.setJarByClass(DistinctByCol.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setPartitionerClass(DistinctPartioner.class);
        job.setMapperClass(DistinctByColMapper.class);
        job.setCombinerClass(DistinctByColCombiner.class);
        job.setReducerClass(DistinctByColReduce.class);
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

        DistinctByColArgs distinctArgs = new DistinctByColArgs();
        int nRet = 0;

        try {
            distinctArgs.init("distinctbycol.jar");
            distinctArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new DistinctByCol(),
                distinctArgs.getDistinctParam());
        System.out.println(nRet);

    }
}
