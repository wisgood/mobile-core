package com.bi.mobilecoredata.middle.user.newactive;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;

import java.util.Map;

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
import com.bi.mobile.playtm.format.dataenum.PlayTMFormatEnum;
import com.bi.mobilecoredata.middle.user.pojo.UserCountInfoEnum;

public class UserNewActiveListMR {

	public static class UserNewActiveMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private final String label = "playtm";

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException,
				UnsupportedEncodingException {
			try {
				String line = value.toString();
				String field[] = line.split("\t");

				// 新增用户表的mac地址
				if (field.length == UserCountInfoEnum.TIMESTAMP.ordinal() + 1) {
					String macout = field[UserCountInfoEnum.MAC.ordinal()];
//					macout = MACFormatUtil.macFormatToCorrectStr(macout);
					context.write(new Text(macout), new Text(line));
				}
				// ETL后播放表的mac地址
				else {
					String mac = field[PlayTMFormatEnum.MACCLEAN.ordinal()].toUpperCase();
					context.write(
							new Text(MACFormatUtil.macFormatToCorrectStr(mac)),
							new Text(label));
				}
			} catch (ArrayIndexOutOfBoundsException e) {

			}
		}
	}

	public static class UserNewActiveReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// System.out.println("*********************************************");
			String hPath = "playtm";
//			String mac = key.toString();
			Map<String, Integer> hashmap = new HashMap<String, Integer>();
			for (Text val : values) {
				String value = val.toString().trim();
				 System.out.println(value);
				hashmap.put(value, 1);
			}
			if (hashmap.containsKey(hPath) && hashmap.size() > 1) {
				Iterator iter = hashmap.entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry<String, Integer> entry = (Map.Entry) iter.next();
					String mapkey = entry.getKey();
					if (!hPath.equalsIgnoreCase(mapkey)) {
						context.write(new Text(mapkey), new Text(""));
					}

				}

			}

		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception,
			InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "UserNewActiveCountMR");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		job.setJarByClass(UserNewActiveListMR.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(UserNewActiveMap.class);
		job.setReducerClass(UserNewActiveReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, "input_active_user");
		FileOutputFormat.setOutputPath(job, new Path("output_active_user"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
