package com.bi.mobilecoredata.middle.fbuffer;

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

import com.bi.mobile.fbuffer.format.dataenum.FbufferFormatEnum;
import com.bi.mobile.playtm.format.dataenum.PlayTMFormatEnum;

public class FbufferonditionMR {

	public static class FbufferonditionMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private static Logger logger = Logger
				.getLogger(FbufferonditionMapper.class.getName());

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String orgiDataStr = value.toString();
				String[] splitSts = orgiDataStr.split("\t");
				String fbufferOKStr = splitSts[FbufferFormatEnum.OK.ordinal()];
				if (fbufferOKStr.equalsIgnoreCase("0")) {
					context.write(
							new Text(splitSts[FbufferFormatEnum.TIMESTAMP
									.ordinal()]), value);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class FbufferonditionReduce extends
			Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				String value = val.toString().trim();
				context.write(new Text(value), NullWritable.get());
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
		job.setJarByClass(FbufferonditionMR.class);
		job.setJobName(" FbufferonditionMR");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		FileInputFormat.setInputPaths(job, new Path("output_fbuffer"));
		FileOutputFormat.setOutputPath(job, new Path(
				"output_fbuffer_ok"));
		job.setMapperClass(FbufferonditionMapper.class);
		job.setReducerClass(FbufferonditionReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
