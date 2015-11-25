package com.bi.mobilecoredata.middle.user.daynewuser;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bi.comm.util.MACFormatUtil;
import com.bi.mobilecoredata.middle.user.pojo.UserCountInfoEnum;

public class UserNewCountFlushMR {

	public static class UserCountMap extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException,
				UnsupportedEncodingException {
			try {
				String line = value.toString();
				String field[] = line.split("\t");
				// System.out.println("length的长度:"+field.length);
				// 历史版本的mac地址
				if (field.length == 3) {
					// System.out.println("大于3"+field.length);
					context.write(new Text(field[0].toUpperCase()), new Text(
							line));
				}
				// 新版本的mac地址
				else {
					String mac = field[UserCountInfoEnum.MAC.ordinal()].toUpperCase();
					//String macout = MACFormatUtil.macFormatToCorrectStr(mac);// mac.replaceAll(":",
					// "");
					context.write(new Text(mac), new Text(line));
				}
			} catch (ArrayIndexOutOfBoundsException e) {

			}
		}
	}

	public static class UserCountReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String newout = null;
//			String historyout = null;
//			String hPath = "history";
//			String nPath = "new";
			boolean label = false;
			boolean exLabel = false;
			for (Text val : values) {
				String value = val.toString();
				String[] field = value.split("\t");
				if (field.length == 3) {
					label = true;
					// historyout = val.toString();
					// System.out.println("historyUser:" + historyout);
				} else {
					exLabel = true;
					newout = val.toString();
					
				}
			}
//			if (label) {
//				context.write(new Text(hPath), new Text(historyout));
//			}
			if (!label && exLabel) {
//				String[] newUserInfoStrs = newout.split("\t");
//				String macout = key.toString();// .replaceAll(":", "");
//				macout = MACFormatUtil.macFormatToCorrectStr(macout);
//				String newHistoryUserInfoStr = macout.toUpperCase() + "\t"
//						+ newUserInfoStrs[UserCountInfoEnum.QUDAO_ID.ordinal()]
//						+ "\t"
//						+ newUserInfoStrs[UserCountInfoEnum.PLAT_ID.ordinal()];
//				context.write(new Text(hPath), new Text(newHistoryUserInfoStr));
				context.write(new Text(newout.trim()), new Text(""));
			}

		}

	}

	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception,
			InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "UserNewOrOldCountFlushMR");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		job.setJarByClass(UserNewCountFlushMR.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(UserCountMap.class);
		job.setReducerClass(UserCountReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, "input_old,input_user");
		FileOutputFormat.setOutputPath(job, new Path("output_new_user_b"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
