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
import com.bi.client.datadefine.PlayBufferingEnum;
import com.bi.client.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;
import com.bi.client.jargsparser.CalculateMRArgsProcessor;
import com.bi.client.util.PNCalculation;
import com.bi.client.util.PNCalculation.PNCalculationException;

/**
 * 
 * @author wangzg
 * @DESC: Sum by Columns Globally
 * 
 */
public class PNByColsGlyMR extends Configured implements Tool
{

	public static class PNByColsGlyMapper extends Mapper<LongWritable, Text, Text, Text>
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

			if ("PlayBufferingEnum".equals(this.enumName))
			{
				long ok = Long.parseLong(this.formatFields[PlayBufferingEnum.OK_FORMAT.ordinal()]);

				if (1 == ok)
				{
					// 联合维度列
					StringBuilder keysValueBuilder = new StringBuilder();

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
						keysValueBuilder.append(fieldValue);
						keysValueBuilder.append("\t");
					}

					// 求PN指标列(只支持单列)
					fieldName = this.distNameArray[0];

					try
					{
						fieldIndex = CustomEnumNameSet.getCustomEnumFieldOrder(enumName, fieldName);
					}
					catch (CustomEnumFieldNotFoundException e)
					{
						throw new InterruptedException(e.getMessage());
					}
					fieldValue = this.formatFields[fieldIndex];

					context.write(new Text(keysValueBuilder.toString().trim()),
							new Text(fieldValue.trim()));
				}
			}
		}

	}

	public static class PNByColsGlyReducer extends Reducer<Text, Text, Text, Text>
	{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException
		{
			long recordNum = 0;
			int pnIndsNum = 0;

			Map<Long, Long> pnIndsFrequenceMap = new HashMap<Long, Long>();

			for (Text indValue : values)
			{
				// 1.计算当前维度下(key)的总记录数
				recordNum++;

				// 2.将指标值放进map, 并统计其在当前维度下(key)出现的次数
				long indicator = Long.parseLong(indValue.toString());

				if (!pnIndsFrequenceMap.containsKey(indicator))
				{
					pnIndsFrequenceMap.put(indicator, 1L);
					// pn值得个数累加1
					pnIndsNum++;
				}
				else
				{
					long frequency = pnIndsFrequenceMap.get(indicator);
					frequency++;
					pnIndsFrequenceMap.put(indicator, frequency);
				}
			}

			try
			{
				String pnStr = PNCalculation.pnCalculate(pnIndsFrequenceMap, pnIndsNum, recordNum);
				//String pnStr = PNCalculation.pnIndicators(pnIndsFrequenceMap, pnIndsNum, recordNum);
				context.write(key, new Text(pnStr));
			}
			catch (PNCalculationException e)
			{
				e.printStackTrace();
			}
		}
	}

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		Job job = new Job(conf);

		job.setJobName("PNByColsGlyMR");
		job.setJarByClass(PNByColsGlyMR.class);

		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.getConfiguration().set(CommonEnum.DISTINDICATORS.name().toLowerCase(), argv[4]);

		job.setMapperClass(PNByColsGlyMapper.class);
		job.setReducerClass(PNByColsGlyReducer.class);
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
		nRet = ToolRunner.run(new Configuration(), new PNByColsGlyMR(),
				argsProcessor.getOptionsValueArray());
		System.out.println(nRet);
	}

}
