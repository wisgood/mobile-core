package com.bi.comm.calculate.sumbycol;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.mobile.comm.constant.ConstantEnum;


/**
 * 
 * @author fuys
 * 
 */
public class SumByColMRUTL {

	public static class SumByColMap extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		private static Logger logger = Logger.getLogger(SumByColMap.class
				.getName());
		private String[] colNumKeys = null;
		private String sumCol = null;

		@Override
		public void setup(Context context) {
			String colNumKeyLine = context.getConfiguration().get(
					ConstantEnum.COLUMNSUMKEY.name());
			this.colNumKeys = colNumKeyLine.split(",");
			this.sumCol = context.getConfiguration().get(
					ConstantEnum.SUMCOL.name());
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String[] field = value.toString().trim().split("\t");

				double sumDouble = Double.parseDouble(field[Integer
						.parseInt(this.sumCol)]);
				long sumLong = (long) sumDouble;
				StringBuilder sbKey = new StringBuilder();
				for (int i = 0; i < colNumKeys.length; i++) {
					String colNumKey = colNumKeys[i];
					sbKey.append(field[Integer.parseInt(colNumKey)]);
					if (i < colNumKeys.length - 1) {
						sbKey.append("\t");
					}
				}
				context.write(new Text(sbKey.toString().trim()), new LongWritable(
						sumLong));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
				logger.error(e.getMessage(), e.getCause());
			}
		}
	}

	public static class SumByColReduce extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(SumByColMRUTL.class);
		job.setJobName("SumByColMRUTL");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		// 设置计算维度数量
		job.getConfiguration().set(
				ConstantEnum.COLUMNSUMKEY.name(),
				"0,6");
		// 设置做sum计算的字段名
		job.getConfiguration().set(ConstantEnum.SUMCOL.name(),
				"21");
		FileInputFormat.addInputPath(job, new Path("output_playtm"));
		FileOutputFormat.setOutputPath(job, new Path("output_sum_playtm"));
		job.setMapperClass(SumByColMap.class);
		job.setReducerClass(SumByColReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
