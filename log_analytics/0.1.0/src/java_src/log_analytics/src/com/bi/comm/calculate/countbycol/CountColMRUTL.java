package com.bi.comm.calculate.countbycol;

import java.io.IOException;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;



public class CountColMRUTL {

	public static class CountColMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static Logger logger = Logger.getLogger(CountColMapper.class
				.getName());

		private final static IntWritable ONE = new IntWritable(1);

		private String[] colNum = null;

		private String delim = null;

		@Override
		public void setup(Context context) {
			this.colNum = context.getConfiguration().get("column").split(",");
			this.delim = context.getConfiguration().get("delim");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String colValue = "";
			String[] field = value.toString().trim().split(this.delim);
			if (colNum.length > 1) {
				for (int i = 0; i < colNum.length; i++) {
					colValue += field[Integer.parseInt(colNum[i])];
					if (i < colNum.length - 1) {
						colValue += "\t";
					}
				}
			} else {
				if (field.length > Integer.parseInt(colNum[0])) {
					colValue = field[Integer.parseInt(colNum[0])];
				}
			}

			context.write(new Text(colValue), ONE);
		}
	}

	/**
	 * reduce : sum
	 */

	public static class CountColReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
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

		String delim = "\t";
		String countInputPaths = "input_langding";
		Job job = new Job();
		job.setJarByClass(CountColMRUTL.class);
		job.setJobName("CountColMRUTL");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		job.getConfiguration().set("column", "2");
		job.getConfiguration().set("delim", delim);
		FileInputFormat.setInputPaths(job, countInputPaths);
		FileOutputFormat.setOutputPath(job, new Path("output_landing_count"));
		job.setMapperClass(CountColMapper.class);
		job.setCombinerClass(CountColReducer.class);
		job.setReducerClass(CountColReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
