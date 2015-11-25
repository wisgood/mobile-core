package com.bi.ios.exchange.calculate;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import com.bi.ios.exchange.datadefine.CommonEnum;
import com.bi.ios.exchange.jargsparser.CalculateMRArgsProcessor;

public class ExtrNewAddMR extends Configured implements Tool
{
	public static class ExtrNewAddMap extends Mapper<LongWritable, Text, Text, Text>
	{

		private String[] formatFields;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException
		{
			this.formatFields = value.toString().trim().split("\t");

			String mac = this.formatFields[0].toUpperCase();

			if (3 == this.formatFields.length)
			{
				context.write(new Text(mac), new Text("#####"));
			}
			else if (2 == this.formatFields.length)
			{
				context.write(new Text(mac), new Text(this.formatFields[1]));
			}
		}
	}

	public static class ExtrNewAddReduce extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException
		{
			Set<String> distIndsSet = new HashSet<String>();
			//StringBuilder indsBuilder = new StringBuilder();

			for (Text val : values)
			{
				distIndsSet.add(val.toString());
				//indsBuilder.append(val.toString());
				//indsBuilder.append("#");
			}

			if (!distIndsSet.contains("#####"))
			{
				StringBuilder indsJoined = new StringBuilder();

				for (String ind : distIndsSet)
				{
					indsJoined.append(ind);
					indsJoined.append("#");
				}

				context.write(new Text(indsJoined.substring(0, indsJoined.length() - 1)), null);
				//context.write(key, new Text(indsJoined.substring(0, indsJoined.length() - 1)));
				//context.write(key, new Text(indsBuilder.substring(0, indsBuilder.length() - 1)));
			}
		}
	}

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();

		Job job = new Job(conf);
		job.setJobName("ExtrNewAddMR");
		job.setJarByClass(ExtrNewAddMR.class);

		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.getConfiguration().set(CommonEnum.DISTINDICATORS.name().toLowerCase(), argv[4]);

		job.setMapperClass(ExtrNewAddMap.class);
		job.setReducerClass(ExtrNewAddReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	public static void main(String[] argv) throws Exception
	{
		CalculateMRArgsProcessor argsProcessor = new CalculateMRArgsProcessor();
		try
		{
			argsProcessor.initDefaultOptions("log_analytics_ios_exchange.jar");
			argsProcessor.parseAndCheckArgs(argv);
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			argsProcessor.getMapRedOptionsParser().printMRJarUsage();
			System.out.println("--- Exit from main ---");
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new ExtrNewAddMR(),
				argsProcessor.getOptionsValueArray());
		System.out.println(nRet);
	}

}
