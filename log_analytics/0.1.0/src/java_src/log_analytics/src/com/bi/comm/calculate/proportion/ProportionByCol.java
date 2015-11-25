package com.bi.comm.calculate.proportion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.calculate.proportion.ProportionByColMRUTL.ProportionByColMapper;
import com.bi.comm.calculate.proportion.ProportionByColMRUTL.ProportionByColReducer;

public class ProportionByCol extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "ProportionByColMRUTL");
        job.setJarByClass(ProportionByCol.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("groupby", args[2]);
        job.getConfiguration().set("numerator", args[3]);
        job.getConfiguration().set("cacul", args[4]);
        job.setMapperClass(ProportionByColMapper.class);
        job.setReducerClass(ProportionByColReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        ProportionByColArgs proportionArgs = new ProportionByColArgs();
        int nRet = 0;

        try {
            proportionArgs.init("proportionbycol.jar");
            proportionArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new ProportionByCol(),
                proportionArgs.getProportionParam());
        System.out.println(nRet);

    }
}
