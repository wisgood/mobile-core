package com.bi.ibidian.calculate;

import java.io.IOException;

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

import com.bi.ibidian.datadefine.CommonEnum;
import com.bi.ibidian.datadefine.CustomEnumNameSet;
import com.bi.ibidian.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;
import com.bi.ibidian.jargsparser.CalculateMRArgs;

public class CountByColumnsMR extends Configured implements Tool
{
	//@formatter:off
	public static class CountByColumnsMap extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		private final static LongWritable ONE = new LongWritable(1);

		private String enumName = null;
		private String[] dimsNameArray = null;
		private String[] formatFields;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			this.enumName = context.getConfiguration()
					.get(CommonEnum.ENUMNAME.name().toLowerCase());
			
			this.dimsNameArray = context.getConfiguration()
					.get(CommonEnum.DIMENSIONS.name().toLowerCase()).split(",");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			StringBuilder dimsValueBuffer = new StringBuilder();

			this.formatFields = value.toString().split("\t");

			for (int i = 0; i < this.dimsNameArray.length; i++)
			{
				String dimFieldName = this.dimsNameArray[i];
				int dimIndex;
				String dimValue;
				try
				{
					dimIndex = CustomEnumNameSet.getCustomEnumFieldOrder(enumName, dimFieldName);
				}
				catch (CustomEnumFieldNotFoundException e)
				{
					throw new InterruptedException(e.getMessage());
				}
				dimValue = formatFields[dimIndex];
				dimsValueBuffer.append(dimValue);
				dimsValueBuffer.append("\t");
			}

			context.write(new Text(dimsValueBuffer.toString().trim()), ONE);
		}

	}

	public static class CountByColumnsReduce extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum = 0;
			for (LongWritable val : values)
			{
				sum += val.get();
			}
			context.write(key, new LongWritable(sum));
		}

	}
	//@formatter:on

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("CountByColumnsMR");
		job.setJarByClass(CountByColumnsMR.class);
		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.setMapperClass(CountByColumnsMap.class);
		job.setCombinerClass(CountByColumnsReduce.class);
		job.setReducerClass(CountByColumnsReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		CalculateMRArgs mrArgs = new CalculateMRArgs();
		try
		{
			mrArgs.initParserOptions("countbycolumnsmr.jar");
			mrArgs.parseArgs(args);
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			mrArgs.getAutoHelpParser().printFuncUsage();
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new CountByColumnsMR(),
				mrArgs.getOptionValueArray());
		System.out.println(nRet);
	}

}
