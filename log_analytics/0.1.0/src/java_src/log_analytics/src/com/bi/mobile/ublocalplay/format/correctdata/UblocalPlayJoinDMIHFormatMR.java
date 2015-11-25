package com.bi.mobile.ublocalplay.format.correctdata;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bi.mobile.comm.dm.pojo.DMInforHashEnum;
import com.bi.mobile.ublocalplay.format.dataenum.UblocalPlayOtherFormatEnum;

public class UblocalPlayJoinDMIHFormatMR {

	public static class UblocalPlayJoinDMIHFormatMap extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException,
				UnsupportedEncodingException {
			try {
				String line = value.toString();
				String[] fields = line.split("\t");

				// dm_inforhash
				if (fields.length == DMInforHashEnum.CHANNEL_ID.ordinal() + 1) {
					String inforhashlinebycomma = line.replaceAll("\t", ",");

					context.write(new Text(fields[DMInforHashEnum.IH.ordinal()]
							.trim().toLowerCase()), new Text(
							inforhashlinebycomma));
				}
				// ublocalplay
				else if(fields.length > UblocalPlayOtherFormatEnum.IH.ordinal()){
					String inforhash = fields[UblocalPlayOtherFormatEnum.IH.ordinal()];
					context.write(new Text(inforhash.trim().toLowerCase()),
							new Text(line));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
	}

	public static class UblocalPlayJoinDMIHFormatReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String inforhashStr = null;
			List<String> ublocalplayInfoList = new ArrayList<String>();
			for (Text val : values) {
				String value = val.toString().trim();
				if (value.contains(",")) {
					inforhashStr = value;

				} else {

					ublocalplayInfoList.add(value);
				}
			}
			for (String ublocalplayInfoString : ublocalplayInfoList) {
				String ublocalplayETLValue = "";
				String[] splitDownloadSts = ublocalplayInfoString.split("\t");
				List<String> splitUblocalplayList = new ArrayList<String>();
				for (String splitDownload : splitDownloadSts) {

					splitUblocalplayList.add(splitDownload);
				}

				if (null != inforhashStr) {
					String[] inforhashStrs = inforhashStr.split(",");

					splitUblocalplayList
							.add(UblocalPlayOtherFormatEnum.CITY_ID.ordinal(),
									inforhashStrs[DMInforHashEnum.CHANNEL_ID
											.ordinal()]);
					splitUblocalplayList.add(
							UblocalPlayOtherFormatEnum.PROVINCE_ID.ordinal() + 1,
							inforhashStrs[DMInforHashEnum.MEIDA_ID.ordinal()]);
					splitUblocalplayList.add(
							UblocalPlayOtherFormatEnum.PROVINCE_ID.ordinal() + 2,
							inforhashStrs[DMInforHashEnum.SERIAL_ID.ordinal()]);

				} else {

					splitUblocalplayList.add(UblocalPlayOtherFormatEnum.CITY_ID.ordinal(),
							"-1");
					splitUblocalplayList
							.add(UblocalPlayOtherFormatEnum.PROVINCE_ID.ordinal() + 1,
									"-1");
					splitUblocalplayList
							.add(UblocalPlayOtherFormatEnum.PROVINCE_ID.ordinal() + 2,
									"-1");

				}
				for (int i = 0; i < splitUblocalplayList.size(); i++) {

					ublocalplayETLValue += splitUblocalplayList.get(i);
					if (i < splitUblocalplayList.size()) {
						ublocalplayETLValue += "\t";
					}
				}

				context.write(new Text(ublocalplayETLValue), new Text(""));
			}

		}
	}
	public static void main(String[] args) throws IOException,
	InterruptedException, ClassNotFoundException {
// TODO Auto-generated method stub
Job job = new Job();
job.setJarByClass(UblocalPlayJoinDMIHFormatMR.class);
job.setJobName("ublocalplayJoinDMVIDFormatETL");
job.getConfiguration().set("mapred.job.tracker", "local");
job.getConfiguration().set("fs.default.name", "local");

FileInputFormat.addInputPaths(job, "output_ublocalplay_new,conf/dm_common_infohash");
FileOutputFormat.setOutputPath(job, new Path("output_ublocalplay_joinih"));
job.setMapperClass(UblocalPlayJoinDMIHFormatMap.class);
job.setReducerClass(UblocalPlayJoinDMIHFormatReduce.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}


}
