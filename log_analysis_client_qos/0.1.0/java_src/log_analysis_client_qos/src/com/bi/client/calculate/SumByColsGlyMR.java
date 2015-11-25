package com.bi.client.calculate;

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

import com.bi.client.datadefine.CommonEnum;
import com.bi.client.datadefine.CustomEnumNameSet;
import com.bi.client.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;
import com.bi.client.jargsparser.CalculateMRArgsProcessor;

/**
 * 
 * @author wangzg
 * @DESC: Sum by Columns Globally
 * 
 */
public class SumByColsGlyMR extends Configured implements Tool
{

	public static class SumByColsGlyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		private String enumName = null;
		private String[] dimsNameArray = null;
		private String[] distNameArray = null;

		private String[] formatFields = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			this.enumName = context.getConfiguration()
					.get(CommonEnum.ENUMNAME.name().toLowerCase()).trim();
			this.dimsNameArray = context.getConfiguration()
					.get(CommonEnum.DIMENSIONS.name().toLowerCase()).trim().split(",");
			this.distNameArray = context.getConfiguration()
					.get(CommonEnum.DISTINDICATORS.name().toLowerCase()).trim().split(",");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException
		{
			this.formatFields = value.toString().trim().split("\t");

			String fieldName;
			int fieldIndex;
			String fieldValue;

			// 联合维度列
			StringBuilder keysValueBuilder = new StringBuilder();

			for (int i = 0; i < this.dimsNameArray.length; ++i)
			{
				fieldName = this.dimsNameArray[i];

				try
				{
					fieldIndex = CustomEnumNameSet.getCustomEnumFieldOrder(enumName, fieldName);
				}
				catch (CustomEnumFieldNotFoundException e)
				{
					throw new InterruptedException(e.getMessage());
				}
				fieldValue = this.formatFields[fieldIndex];
				keysValueBuilder.append(fieldValue);
				keysValueBuilder.append("\t");
			}

			// 联合求和指标列
			StringBuilder indsValueBuilder = new StringBuilder();

			for (int i = 0; i < this.distNameArray.length; i++)
			{
				fieldName = this.distNameArray[i];

				try
				{
					fieldIndex = CustomEnumNameSet.getCustomEnumFieldOrder(enumName, fieldName);
				}
				catch (CustomEnumFieldNotFoundException e)
				{
					throw new InterruptedException(e.getMessage());
				}
				fieldValue = this.formatFields[fieldIndex];
				indsValueBuilder.append(fieldValue);
				indsValueBuilder.append("\t");
			}

			context.write(new Text(keysValueBuilder.toString().trim()), new Text(indsValueBuilder
					.toString().trim()));
		}

	}

	public static class SumByColsGlyReducer extends Reducer<Text, Text, Text, Text>
	{
		private String[] distNameArray = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			this.distNameArray = context.getConfiguration()
					.get(CommonEnum.DISTINDICATORS.name().toLowerCase()).trim().split(",");
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException
		{
			long[] indsSumArray = new long[this.distNameArray.length];

			for (int i = 0; i < this.distNameArray.length; i++)
			{
				indsSumArray[i] = 0;
			}

			for (Text value : values)
			{
				String[] indsArray = value.toString().trim().split("\t");

				for (int i = 0; i < this.distNameArray.length; i++)
				{
					indsSumArray[i] += Long.parseLong(indsArray[i]);
				}
			}

			StringBuilder indsValueBuilder = new StringBuilder();

			for (int i = 0; i < this.distNameArray.length; i++)
			{
				indsValueBuilder.append(indsSumArray[i]);
				indsValueBuilder.append("\t");
			}

			context.write(key, new Text(indsValueBuilder.toString().trim()));
		}

	}

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		Job job = new Job(conf);

		job.setJobName("SumByColsGlyMR");
		job.setJarByClass(SumByColsGlyMR.class);

		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.getConfiguration().set(CommonEnum.DISTINDICATORS.name().toLowerCase(), argv[4]);

		job.setMapperClass(SumByColsGlyMapper.class);
		job.setReducerClass(SumByColsGlyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(7);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	public static void main(String[] argv) throws Exception
	{
		CalculateMRArgsProcessor argsProcessor = new CalculateMRArgsProcessor();
		try
		{
			argsProcessor.initDefaultOptions("log_analytics_client_qos.jar");
			argsProcessor.parseAndCheckArgs(argv);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			argsProcessor.getMapRedOptionsParser().printMRJarUsage();
			System.out.println("--- Exit from main ---");
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new SumByColsGlyMR(),
				argsProcessor.getOptionsValueArray());
		System.out.println(nRet);
	}

}
