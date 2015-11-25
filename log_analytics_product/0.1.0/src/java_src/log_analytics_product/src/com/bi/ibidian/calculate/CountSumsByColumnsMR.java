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

public class CountSumsByColumnsMR extends Configured implements Tool
{
	//@formatter:off
	public static class CountSumsByColumnsMap extends Mapper<LongWritable, Text, Text, Text>
	{
		private final static LongWritable ONE = new LongWritable(1);

		private String enumName = null;
		private String[] dimsNameArray = null;
		private String[] indsNameArray = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			this.enumName = context.getConfiguration()
					.get(CommonEnum.ENUMNAME.name().toLowerCase());
			this.dimsNameArray = context.getConfiguration()
					.get(CommonEnum.DIMENSIONS.name().toLowerCase()).split(",");
			this.indsNameArray = context.getConfiguration()
					.get(CommonEnum.INDICATORS.name().toLowerCase()).split(",");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			StringBuilder dimsValueBuffer = new StringBuilder();
			StringBuilder indsValueBuffer = new StringBuilder();

			String[] formatFields = value.toString().split("\t");

			for (int i = 0; i < dimsNameArray.length; i++)
			{
				String dimFieldName = dimsNameArray[i];
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

			for (int i = 0; i < indsNameArray.length; i++)
			{
				String indFieldName = indsNameArray[i];
				int indIndex;
				long indValue;
				try
				{
					indIndex = CustomEnumNameSet.getCustomEnumFieldOrder(enumName, indFieldName);
				}
				catch (CustomEnumFieldNotFoundException e)
				{
					throw new InterruptedException(e.getMessage());
				}
				indValue = Long.parseLong(formatFields[indIndex]);
				if (indValue < 0)
				{
					indValue = 0;
				}
				indsValueBuffer.append(indValue);
				indsValueBuffer.append("\t");
			}
			indsValueBuffer.append(ONE);

			context.write(new Text(dimsValueBuffer.toString().trim()), new Text(indsValueBuffer.toString().trim()));
		}
	}

	public static class CountSumsByColumnsReduce extends Reducer<Text, Text, Text, Text>
	{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			long[] sumArray = null;
			StringBuilder indsValueBuffer = new StringBuilder();
			
			for (Text val : values)
			{
				String[] indsValues = val.toString().trim().split("\t");
				int indsNum = indsValues.length;
				if (null == sumArray)
				{
					sumArray = new long[indsNum];
				}
				for (int i = 0; i < indsNum; i++)
				{
					sumArray[i] += Long.parseLong(indsValues[i]);
				}
			}

			for (int i = 0; i < sumArray.length; i++)
			{
				indsValueBuffer.append(sumArray[i]);
				indsValueBuffer.append("\t");
			}

			context.write(key, new Text(indsValueBuffer.toString().trim()));
		}

	}
	//@formatter:on

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("CountSumsByColumnsMR");
		job.setJarByClass(CountSumsByColumnsMR.class);
		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.getConfiguration().set(CommonEnum.INDICATORS.name().toLowerCase(), argv[4]);
		job.setMapperClass(CountSumsByColumnsMap.class);
		job.setCombinerClass(CountSumsByColumnsReduce.class);
		job.setReducerClass(CountSumsByColumnsReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		CalculateMRArgs mrArgs = new CalculateMRArgs();
		try
		{
			mrArgs.initParserOptions("countsumsbycolumnsmr.jar");
			mrArgs.parseArgs(args);
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			mrArgs.getAutoHelpParser().printFuncUsage();
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new CountSumsByColumnsMR(),
				mrArgs.getOptionValueArray());
		System.out.println(nRet);
	}

}
