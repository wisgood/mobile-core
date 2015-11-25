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

public class CDByColsLlyMR_Popup extends Configured implements Tool
{

	public static class CDByColsLly_PopupMap extends Mapper<LongWritable, Text, Text, Text>
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

			StringBuilder keysValueBuffer = new StringBuilder();

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

			StringBuilder indsValueBuffer = new StringBuilder();
			fieldName = "O_TAB";
			try
			{
				fieldIndex = CustomEnumNameSet.getCustomEnumFieldOrder(enumName, fieldName);
			}
			catch (CustomEnumFieldNotFoundException e)
			{
				throw new InterruptedException(e.getMessage());
			}

			fieldValue = this.formatFields[fieldIndex];

			int popupNum = 0;
			int recordNum = 1;

			if ("启动".equals(fieldValue))
			{
				popupNum = 1;
			}
			indsValueBuffer.append(popupNum);
			indsValueBuffer.append("\t");
			indsValueBuffer.append(recordNum);

			context.write(new Text(keysValueBuffer.toString().trim()), new Text(indsValueBuffer
					.toString().trim()));
		}

	}

	public static class CDByColsLly_PopupReduce extends Reducer<Text, Text, Text, Text>
	{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException
		{
			long popupNumSum = 0;
			long recordNumSum = 0;

			for (Text indsJoined : values)
			{
				String[] indsArray = indsJoined.toString().split("\t");
				popupNumSum += Long.parseLong(indsArray[0]);
				recordNumSum += Long.parseLong(indsArray[1]);
			}

			StringBuilder indsValueBuffer = new StringBuilder();
			indsValueBuffer.append(popupNumSum);
			indsValueBuffer.append("\t");
			indsValueBuffer.append(recordNumSum);

			context.write(key, new Text(indsValueBuffer.toString().trim()));
		}

	}

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		Job job = new Job(conf);

		job.setJobName("CDByColsLlyMR_Popup");
		job.setJarByClass(CDByColsLlyMR_Popup.class);

		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.getConfiguration().set(CommonEnum.DISTINDICATORS.name().toLowerCase(), argv[4]);

		job.setMapperClass(CDByColsLly_PopupMap.class);
		job.setReducerClass(CDByColsLly_PopupReduce.class);
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
		nRet = ToolRunner.run(new Configuration(), new CDByColsLlyMR_Popup(),
				argsProcessor.getOptionsValueArray());
		System.out.println(nRet);
	}

}
