package com.bi.comm.calculate.countsingle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.calculate.countsingle.CountSingleMRUTL.CountSingleMapper;
import com.bi.comm.calculate.countsingle.CountSingleMRUTL.CountSingleReducer;



public class CountSingle extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
	
		Job job = new Job(conf, "Count");
		job.setJarByClass(CountSingle.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(CountSingleMapper.class);
		job.setCombinerClass(CountSingleReducer.class);
		job.setReducerClass(CountSingleReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		CountSingleArgs countArgs = new CountSingleArgs();
		int nRet = 0;

	
		try {
			countArgs.init("countbysingle.jar");
			countArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			// countArgs.parser.printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(), new CountSingle(),
				countArgs.getCountParam());
		System.out.println(nRet);

	}
}
