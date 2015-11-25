package com.bi.mobilecoredata.middle.play;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.bi.mobile.playtm.format.dataenum.PlayTMFormatEnum;

public class PlayTMConditionMR {

	public static class PlayTMConditionMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private static Logger logger = Logger
				.getLogger(PlayTMConditionMapper.class.getName());

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String orgiDataStr = value.toString();
				String[] splitSts = orgiDataStr.split("\t");
				String mac = splitSts[PlayTMFormatEnum.MAC.ordinal()]
						.toUpperCase();
				if (mac.contains(":")) {
					mac = mac.replaceAll(":", "");
				}
				context.write(new Text(mac), value);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class PlayTMConditionReduce extends
			Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String orgiDataStr = null;
			long vtmlongsum = 0;
			for (Text value : values) {
				orgiDataStr = value.toString();
				String[] splitSts = orgiDataStr.split("\t");
				if (splitSts.length >= PlayTMFormatEnum.VTM.ordinal()) {
					String vtmStr = splitSts[PlayTMFormatEnum.VTM.ordinal()];
					try {
						double vtmDouble = Double.parseDouble(vtmStr);
						long vtmlong = (long) vtmDouble;
						vtmlongsum += vtmlong;
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			long conditionlong = 4*1000*60;
			if (null != orgiDataStr && vtmlongsum >=conditionlong) {
				context.write(new Text(orgiDataStr), NullWritable.get());
			}

		}

	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(PlayTMConditionMR.class);
		job.setJobName("PlayTMConditionMR");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		FileInputFormat.setInputPaths(job, new Path("output_playtm_new_19"));
		FileOutputFormat.setOutputPath(job, new Path(
				"output_validcondition_playtm"));
		job.setMapperClass(PlayTMConditionMapper.class);
		job.setReducerClass(PlayTMConditionReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
