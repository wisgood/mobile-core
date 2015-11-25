package com.bi.comm.calculate.sumsigle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.calculate.sumsigle.SumSingleMRUTL.SumSingleMapper;
import com.bi.comm.calculate.sumsigle.SumSingleMRUTL.SumSingleReducer;

public class SumSingle extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("sumcol", args[2]);
		Job job = new Job(conf, "SumSingleMRUTL");
		job.setJarByClass(SumSingle.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(SumSingleMapper.class);
		job.setCombinerClass(SumSingleReducer.class);
		job.setReducerClass(SumSingleReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(LongWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
       SumSingleArgs sumSingleArgs = new SumSingleArgs();
		int nRet = 0;
		try {
			sumSingleArgs.init("sumsingle.jar");
			sumSingleArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			//sumSingleArgs.get;
			e.printStackTrace();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(), new SumSingle(),
				sumSingleArgs.getSumParam());
		System.out.println(nRet);

	}
}
