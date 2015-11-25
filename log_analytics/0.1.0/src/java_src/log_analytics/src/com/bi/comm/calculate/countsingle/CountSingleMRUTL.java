package com.bi.comm.calculate.countsingle;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountSingleMRUTL {

	/**
	 * map : map every record to IntWritable 1
	 */

	public static class CountSingleMapper extends
			Mapper<LongWritable, Text, NullWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(NullWritable.get(), one);
		}
	}

	/**
	 * reduce : sum
	 */

	public static class CountSingleReducer extends
			Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {

		public void reduce(NullWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(NullWritable.get(), new IntWritable(sum));
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(CountSingleMRUTL.class);
		job.setJobName("CountSingleMRUTL");
	    job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		FileInputFormat.addInputPath(job, new Path("output_distinct_comdm"));
		FileOutputFormat.setOutputPath(job, new Path("output_day_usercount"));
		job.setMapperClass(CountSingleMapper.class);
		job.setCombinerClass(CountSingleReducer.class);
		job.setReducerClass(CountSingleReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
