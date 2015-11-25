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
import com.bi.ios.exchange.datadefine.CustomEnumNameSet;
import com.bi.ios.exchange.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

public class ExtrDistFieldsMR extends Configured implements Tool
{
	public static class ExtrDistFieldsLlyMap extends Mapper<LongWritable, Text, Text, Text>
	{
		private String enumName;
		private String[] dimsNameArray;
		private String[] distNameArray;

		private String[] formatFields;

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

			StringBuilder keysValueBuilder = new StringBuilder();
			String keysJoined;

			for (int i = 0; i < this.dimsNameArray.length; i++)
			{
				fieldName = this.dimsNameArray[i];

				try
				{
					fieldIndex = CustomEnumNameSet
							.getCustomEnumFieldOrder(this.enumName, fieldName);
				}
				catch (CustomEnumFieldNotFoundException e)
				{
					throw new InterruptedException(e.getMessage());
				}

				fieldValue = this.formatFields[fieldIndex];

				if (null == fieldValue || "".equals(fieldValue))
				{
					fieldValue = "UNDEFINED";
				}
				else
				{
					fieldValue = fieldValue.replaceAll("[^a-zA-Z0-9\\.]", "");

					if (null == fieldValue || "".equals(fieldValue))
					{
						fieldValue = "UNDEFINED";
					}
					else
					{
						fieldValue = fieldValue.toUpperCase();
					}
				}

				keysValueBuilder.append(fieldValue);
				keysValueBuilder.append("\t");
			}

			keysJoined = keysValueBuilder.toString().trim();

			StringBuilder indsValueBuilder = new StringBuilder();
			String indsJoined;

			for (int i = 0; i < this.distNameArray.length; i++)
			{
				fieldName = this.distNameArray[i];

				try
				{
					fieldIndex = CustomEnumNameSet
							.getCustomEnumFieldOrder(this.enumName, fieldName);
				}
				catch (CustomEnumFieldNotFoundException e)
				{
					throw new InterruptedException(e.getMessage());
				}

				fieldValue = this.formatFields[fieldIndex];

				if (null == fieldValue || "".equals(fieldValue))
				{
					fieldValue = "UNDEFINED";
				}
				else
				{
					fieldValue = fieldValue.replaceAll("[^a-zA-Z0-9\\.]", "");

					if (null == fieldValue || "".equals(fieldValue))
					{
						fieldValue = "UNDEFINED";
					}
					else
					{
						fieldValue = fieldValue.toUpperCase();
					}
				}

				indsValueBuilder.append(fieldValue);
				indsValueBuilder.append(":");
			}

			indsJoined = indsValueBuilder.substring(0, indsValueBuilder.length() - 1);

			context.write(new Text(keysJoined), new Text(indsJoined));
		}
	}

	public static class ExtrDistFieldsReduce extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException
		{
			Set<String> distIndsSet = new HashSet<String>();
			for (Text val : values)
			{
				distIndsSet.add(val.toString());
			}
			StringBuilder indsJoined = new StringBuilder();
			for (String ind : distIndsSet)
			{
				indsJoined.append(ind);
				indsJoined.append(",");
			}
			context.write(key, new Text(indsJoined.substring(0, indsJoined.length() - 1)));
		}
	}

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		Job job = new Job(conf);

		job.setJobName("ExtrDistFieldsLlyMR");
		job.setJarByClass(ExtrDistFieldsMR.class);

		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.getConfiguration().set(CommonEnum.DISTINDICATORS.name().toLowerCase(), argv[4]);

		job.setMapperClass(ExtrDistFieldsLlyMap.class);
		job.setReducerClass(ExtrDistFieldsReduce.class);
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
		nRet = ToolRunner.run(new Configuration(), new ExtrDistFieldsMR(),
				argsProcessor.getOptionsValueArray());
		System.out.println(nRet);
	}

}
