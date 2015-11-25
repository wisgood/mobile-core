package com.bi.calculate.dayreport;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
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

public class WebNew extends Configured implements Tool
{
	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] fields = value.toString().trim().split("\t");
			//user_flag pos is [6]; fck pos is [23]; user_flag==1 =>new user
			if(fields.length > 25)
			{
				int userFlag = Integer.parseInt(fields[6]);
				int url1 = Integer.parseInt(fields[7]);
				String fck = fields[23];
				if( 1 == userFlag && 1 == url1 )
				{
					String outKey = fields[0].concat("\t").concat(fck);
					context.write(new Text(outKey), new Text(""));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException 
		{
			context.write(key, new Text(""));
		}
	}

	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		GenericOptionsParser optionParser = new GenericOptionsParser( conf, argv );
		conf = optionParser.getConfiguration();

		Job job = new Job(conf, "DistinctFckMR");
		job.setJarByClass(WebNew.class);

		FileInputFormat.setInputPaths( job, conf.get("inputDir") );
		FileOutputFormat.setOutputPath( job, new Path(conf.get("outputDir")) );

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;	
	}
	
	public static void main(String[] argv) throws Exception
	{
		int nRet = 0;
		nRet = ToolRunner.run( new Configuration(), new WebNew(), argv );
		System.out.println(nRet);
	}	
	
}

