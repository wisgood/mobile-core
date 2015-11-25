package com.bi.client.calculate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import com.bi.client.datadefine.DtfspEnum;
import com.bi.client.datadefine.DtjsEnum;
import com.bi.client.jargsparser.CalculateMRArgsProcessor;

/**
 * 
 * @author wangzg
 * @DESC: Count Distinct by Columns Globally
 * 
 */
public class CDByColsLlyMR_LE0 extends Configured implements Tool
{

	public static class CDByColsLly_LE0_Mapper extends
			Mapper<LongWritable, Text, Text, LongWritable>
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

			if ("DtfspEnum".equals(this.enumName))
			{
				int le = Integer.parseInt(this.formatFields[DtfspEnum.LE_FORMAT.ordinal()]);

				if (0 == le)
				{
					// key的第一部分（真正的维度）
					for (int i = 0; i < this.dimsNameArray.length; ++i)
					{
						fieldName = this.dimsNameArray[i];

						try
						{
							fieldIndex = CustomEnumNameSet.getCustomEnumFieldOrder(enumName,
									fieldName);
						}
						catch (CustomEnumFieldNotFoundException e)
						{
							throw new InterruptedException(e.getMessage());
						}
						fieldValue = this.formatFields[fieldIndex];
						keysValueBuffer.append(fieldValue);
						keysValueBuffer.append("\t");
					}
					// key的第二部分（排重列，相当于--下钻）
					for (int i = 0; i < this.distNameArray.length; i++)
					{
						fieldName = this.distNameArray[i];

						try
						{
							fieldIndex = CustomEnumNameSet.getCustomEnumFieldOrder(enumName,
									fieldName);
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
			else if ("DtjsEnum".equals(this.enumName))
			{
				int le = Integer.parseInt(this.formatFields[DtjsEnum.LE_FORMAT.ordinal()]);

				if (0 == le)
				{
					// key的第一部分（真正的维度）
					for (int i = 0; i < this.dimsNameArray.length; ++i)
					{
						fieldName = this.dimsNameArray[i];

						try
						{
							fieldIndex = CustomEnumNameSet.getCustomEnumFieldOrder(enumName,
									fieldName);
						}
						catch (CustomEnumFieldNotFoundException e)
						{
							throw new InterruptedException(e.getMessage());
						}
						fieldValue = this.formatFields[fieldIndex];
						keysValueBuffer.append(fieldValue);
						keysValueBuffer.append("\t");
					}
					// key的第二部分（排重列，相当于--下钻）
					for (int i = 0; i < this.distNameArray.length; i++)
					{
						fieldName = this.distNameArray[i];

						try
						{
							fieldIndex = CustomEnumNameSet.getCustomEnumFieldOrder(enumName,
									fieldName);
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
		}

	}

	public static class CDByColsLly_LE0_Reducer extends
			Reducer<Text, LongWritable, Text, LongWritable>
	{
		private String[] dimsNameArray;
		private Map<String, Long> recdNumMap = new HashMap<String, Long>();
		private Map<String, Long> distNumMap = new HashMap<String, Long>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			this.dimsNameArray = context.getConfiguration()
					.get(CommonEnum.DIMENSIONS.name().toLowerCase()).trim().split(",");
		}

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException
		{
			// 1. 上卷（将排重列抛掉）
			String[] keysArray = key.toString().trim().split("\t");

			StringBuilder dimsJoinBuilder = new StringBuilder();

			for (int i = 0; i < this.dimsNameArray.length; i++)
			{
				dimsJoinBuilder.append(keysArray[i]);
				dimsJoinBuilder.append("\t");
			}
			String dimsJoined = dimsJoinBuilder.toString().trim();

			if (!recdNumMap.containsKey(dimsJoined))
			{
				recdNumMap.put(dimsJoined, 0L);
				distNumMap.put(dimsJoined, 0L);
			}

			// 2. 分维度计算记录数和排重数
			long recordNumSum = 0;

			for (LongWritable recordNum : values)
			{
				recordNumSum += recordNum.get();
			}

			long tempSum;
			tempSum = recdNumMap.get(dimsJoined) + recordNumSum;
			recdNumMap.put(dimsJoined, tempSum);
			tempSum = distNumMap.get(dimsJoined) + 1;
			distNumMap.put(dimsJoined, tempSum);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{

			for (String dimsJoined : recdNumMap.keySet())
			{
				StringBuilder outputBuilder = new StringBuilder();

				outputBuilder.append(dimsJoined);
				outputBuilder.append("\t");
				outputBuilder.append(recdNumMap.get(dimsJoined));
				outputBuilder.append("\t");
				outputBuilder.append(distNumMap.get(dimsJoined));

				context.write(new Text(outputBuilder.toString().trim()), null);
			}
		}

	}

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		Job job = new Job(conf);

		job.setJobName("CDByColsLlyMR_LE0");
		job.setJarByClass(CDByColsLlyMR_LE0.class);

		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.getConfiguration().set(CommonEnum.DISTINDICATORS.name().toLowerCase(), argv[4]);

		job.setMapperClass(CDByColsLly_LE0_Mapper.class);
		job.setReducerClass(CDByColsLly_LE0_Reducer.class);
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
		nRet = ToolRunner.run(new Configuration(), new CDByColsLlyMR_LE0(),
				argsProcessor.getOptionsValueArray());
		System.out.println(nRet);
	}

}
