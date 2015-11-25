package com.bi.calculate.dayreport;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class CountLines extends Configured implements Tool
{

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String date = value.toString().trim().split("\t")[0];
			Text outKey = new Text(date);
			IntWritable outValue = new IntWritable(1);
			context.write(outKey, outValue);
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values,	Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for(IntWritable val : values)
			{
				++sum;
			}
			context.write(new Text(key), new IntWritable(sum));
		}
	}
	
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		GenericOptionsParser optionParser = new GenericOptionsParser( conf, argv );
		conf = optionParser.getConfiguration();

		Job job = new Job(conf, "CountLines");
		job.setJarByClass(CountLines.class);

		FileInputFormat.setInputPaths( job, conf.get("inputDir") );
		FileOutputFormat.setOutputPath( job, new Path(conf.get("outputDir")) );

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;	
	}
	
	public static void main(String[] argv) throws Exception
	{
		int nRet = 0;
		nRet = ToolRunner.run( new Configuration(), new CountLines(), argv );
		System.out.println(nRet);
	}
}
