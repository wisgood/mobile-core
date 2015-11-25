package com.bi.minisite.calculate;

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

import com.bi.minisite.datadefine.CommonEnum;
import com.bi.minisite.datadefine.CustomEnumNameSet;
import com.bi.minisite.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;
import com.bi.minisite.jargsparser.CalculateMRArgsProcessor;

/**
 * 
 * @author wangzg
 * @DESC: Count Distinct by Columns Locally
 * 
 */
public class CDByColsLlyMR extends Configured implements Tool
{

	public static class CDByColsLlyMap extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		private final LongWritable ONE = new LongWritable(1);
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

			StringBuilder keysValueBuffer = new StringBuilder();
			String fieldName;
			int fieldIndex;
			String fieldValue;

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
				keysValueBuffer.append(fieldValue);
				keysValueBuffer.append("\t");
			}

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
				keysValueBuffer.append(fieldValue);
				keysValueBuffer.append("\t");
			}

			context.write(new Text(keysValueBuffer.toString().trim()), ONE);
		}

	}

	public static class CDByColsLlyReduce extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException
		{
			long recordNumSum = 0;
			for (LongWritable recordNum : values)
			{
				recordNumSum += recordNum.get();
			}

			context.write(key, new LongWritable(recordNumSum));
		}

	}

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		Job job = new Job(conf);

		job.setJobName("CDByColsLlyMR");
		job.setJarByClass(CDByColsLlyMR.class);

		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.getConfiguration().set(CommonEnum.DISTINDICATORS.name().toLowerCase(), argv[4]);

		job.setMapperClass(CDByColsLlyMap.class);
		job.setReducerClass(CDByColsLlyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(7);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	public static void main(String[] argv) throws Exception
	{
		CalculateMRArgsProcessor argsProcessor = new CalculateMRArgsProcessor();
		try
		{
			argsProcessor.initDefaultOptions("log_analytics_product.jar");
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
		nRet = ToolRunner.run(new Configuration(), new CDByColsLlyMR(),
				argsProcessor.getOptionsValueArray());
		System.out.println(nRet);
	}

}
