package com.bi.minisite.calculate;

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

import com.bi.minisite.datadefine.CommonEnum;
import com.bi.minisite.jargsparser.CalculateMRArgsProcessor;

/**
 * 
 * @author wangzg
 * @DESC: Count Distinct by Columns Globally
 * 
 */
public class CDByColsGlyMR extends Configured implements Tool
{

	public static class CDByColsGlyMap extends Mapper<LongWritable, Text, Text, LongWritable>
	{

		private String[] computedFields = null;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException
		{
			this.computedFields = value.toString().trim().split("\t");

			StringBuilder keysValueBuilder = new StringBuilder();

			long recordNumSum;

			int i = 0;

			for (; i < this.computedFields.length - 1; i++)
			{
				keysValueBuilder.append(this.computedFields[i]);
				keysValueBuilder.append("\t");
			}
			recordNumSum = Long.parseLong(this.computedFields[i]);

			context.write(new Text(keysValueBuilder.toString().trim()), new LongWritable(
					recordNumSum));
		}

	}

	public static class CDByColsGlyReduce extends Reducer<Text, LongWritable, Text, LongWritable>
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

			long recordNumSum = 0;
			long distNumSum = 0;

			for (LongWritable recordNum : values)
			{
				distNumSum++;
				recordNumSum += recordNum.get();
			}

			long tempSum;
			tempSum = recdNumMap.get(dimsJoined) + recordNumSum;
			recdNumMap.put(dimsJoined, tempSum);
			tempSum = distNumMap.get(dimsJoined) + distNumSum;
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

		job.setJobName("CDByColsGlyMR");
		job.setJarByClass(CDByColsGlyMR.class);

		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.getConfiguration().set(CommonEnum.DIMENSIONS.name().toLowerCase(), argv[3]);
		job.getConfiguration().set(CommonEnum.DISTINDICATORS.name().toLowerCase(), argv[4]);

		job.setMapperClass(CDByColsGlyMap.class);
		job.setReducerClass(CDByColsGlyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(1);

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
		nRet = ToolRunner.run(new Configuration(), new CDByColsGlyMR(),
				argsProcessor.getOptionsValueArray());
		System.out.println(nRet);
	}

}
