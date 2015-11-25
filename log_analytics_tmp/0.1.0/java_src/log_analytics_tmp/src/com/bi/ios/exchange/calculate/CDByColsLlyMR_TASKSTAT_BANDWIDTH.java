package com.bi.ios.exchange.calculate;

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

import com.bi.ios.exchange.datadefine.CommonEnum;
import com.bi.ios.exchange.datadefine.TaskStatEnum;
import com.bi.ios.exchange.jargsparser.CalculateMRArgsProcessor;
import com.bi.ios.exchange.util.PathProcessor;
import com.bi.ios.exchange.util.TimeStamp;

/**
 * 
 * @author wangzg
 * @DESC: Count Distinct by Columns Globally
 * 
 */
public class CDByColsLlyMR_TASKSTAT_BANDWIDTH extends Configured implements Tool
{

	public static class CDByColsLly_TASKSTAT_Mapper extends
			Mapper<LongWritable, Text, Text, LongWritable>
	{
		private final LongWritable ONE = new LongWritable(1);

		private String enumName = null;

		private String[] formatFields = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			this.enumName = context.getConfiguration()
					.get(CommonEnum.ENUMNAME.name().toLowerCase()).trim();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException
		{
			this.formatFields = value.toString().trim().split("\t");

			StringBuilder keysValueBuffer = new StringBuilder();

			out:
			if ("TaskStatEnum".equals(this.enumName))
			{
				long download_speed = 0;

				try
				{
					download_speed = Long.parseLong(this.formatFields[TaskStatEnum.DOWNLOAD_RATE
							.ordinal()]);
				}
				catch (NumberFormatException e)
				{
					break out;
				}

				String bandWidth = "undefined";

				if (100 > download_speed)
				{
					bandWidth = "speed<100";
				}
				else if ((100 <= download_speed) && (265 > download_speed))
				{
					bandWidth = "100<=speed<265";
				}
				else if ((265 <= download_speed) && (350 > download_speed))
				{
					bandWidth = "265<=speed<350";
				}
				else if ((350 <= download_speed) && (512 > download_speed))
				{
					bandWidth = "350<=speed<512";
				}
				else if ((512 <= download_speed) && (700 > download_speed))
				{
					bandWidth = "512<=speed<700";
				}
				else if (700 <= download_speed)
				{
					bandWidth = "700<=speed";
				}

				String timeStamp = this.formatFields[TaskStatEnum.TIME_STAMP.ordinal()];

				String mac = this.formatFields[TaskStatEnum.MAC_HEX.ordinal()];

				int dateID = TimeStamp.getDateID(timeStamp);

				//long taskNum = download_speed + ftp + ftsd;

				keysValueBuffer.append(dateID);
				keysValueBuffer.append("\t");
				keysValueBuffer.append(bandWidth);
				keysValueBuffer.append("\t");
				keysValueBuffer.append(mac);

				context.write(new Text(keysValueBuffer.toString().trim()), ONE);
			}
		}
	}

	public static class CDByColsLly_TASKSTAT_Reducer extends
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

		String mrPath = CDByColsLlyMR_TASKSTAT_BANDWIDTH.class.getName();
		int lastDotPos = mrPath.lastIndexOf(".") + 1;
		String mrName = mrPath.substring(lastDotPos);

		job.setJobName(mrName);
		job.setJarByClass(CDByColsLlyMR_TASKSTAT_BANDWIDTH.class);

		FileInputFormat.setInputPaths(job, PathProcessor.listHourPaths(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.getConfiguration().set(CommonEnum.DISTINDICATORS.name().toLowerCase(), argv[4]);

		job.setMapperClass(CDByColsLly_TASKSTAT_Mapper.class);
		job.setReducerClass(CDByColsLly_TASKSTAT_Reducer.class);
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
			argsProcessor.initDefaultOptions("log_analytics_ios_exchange.jar");
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
		nRet = ToolRunner.run(new Configuration(), new CDByColsLlyMR_TASKSTAT_BANDWIDTH(),
				argsProcessor.getOptionsValueArray());
		System.out.println(nRet);
	}

}
