package com.bi.mobile.evideo.format.correctdata;

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


import com.bi.mobile.comm.dm.pojo.DMVideoIDEnum;
import com.bi.mobile.evideo.format.dataenum.EvideoOutVIDFormatEnum;

public class EvideoJoinDMVIDFormatMR {
	
	public static class EvideoJoinDMVIDFormatMap extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException,
				UnsupportedEncodingException {
			try {
				String line = value.toString();
				String[] fields = line.split("\t");

				// video information
				if (fields.length == DMVideoIDEnum.VIDEONAME.ordinal() + 1) {
					String videolinebycomma = line.replaceAll("\t", ",");
          
					context.write(new Text(fields[DMVideoIDEnum.VIDEOID.ordinal()]), new Text(
							videolinebycomma));
				}
				// evideo
				else {
					String videoid = getVIDUtil.getVID(fields);
					context.write(new Text(videoid),
							new Text(line));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
	}

	public static class EvideoJoinDMVIDFormatReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String videoDMInfoStr = null;
			List<String> evideoInfoList = new ArrayList<String>();
			for (Text val : values) {
				String value = val.toString().trim();
				if (value.contains(",")) {
					videoDMInfoStr = value;

				} else {
					evideoInfoList.add(value);
				}
			}
			for (String evideoInfoString : evideoInfoList) {
				String evideoETLValue = "";
				String[] splitEvideoSts = evideoInfoString.split("\t");
				List<String> splitEvideoList = new ArrayList<String>();
				for (String splitEvideo : splitEvideoSts) {

					splitEvideoList.add(splitEvideo);
				}

				if (null != videoDMInfoStr) {
					String[] videoStrs = videoDMInfoStr.split(",");

					splitEvideoList
							.add(EvideoOutVIDFormatEnum.CITY_ID.ordinal(),
									videoStrs[DMVideoIDEnum.CHANNEL_ID
											.ordinal()]);
					splitEvideoList.add(
							EvideoOutVIDFormatEnum.PROVINCE_ID.ordinal() + 1,
							videoStrs[DMVideoIDEnum.VIDEOID.ordinal()]);
					splitEvideoList.add(
							EvideoOutVIDFormatEnum.PROVINCE_ID.ordinal() + 2,
							videoStrs[DMVideoIDEnum.VIDEONAME.ordinal()]);

				} else {

					splitEvideoList.add(EvideoOutVIDFormatEnum.CITY_ID.ordinal(),
							"-1");
					splitEvideoList
							.add(EvideoOutVIDFormatEnum.PROVINCE_ID.ordinal() + 1,
									"-1");
					splitEvideoList
							.add(EvideoOutVIDFormatEnum.PROVINCE_ID.ordinal() + 2,
									"-1");

				}
				for (int i = 0; i < splitEvideoList.size(); i++) {

					evideoETLValue += splitEvideoList.get(i);
					if (i < splitEvideoList.size()) {
						evideoETLValue += "\t";
					}
				}

				context.write(new Text(evideoETLValue), new Text(""));
			}

		}
	}
	public static void main(String[] args) throws IOException,
	InterruptedException, ClassNotFoundException {
// TODO Auto-generated method stub
Job job = new Job();
job.setJarByClass(EvideoJoinDMVIDFormatMR.class);
job.setJobName("EvideoJoinDMVIDFormatETL");
job.getConfiguration().set("mapred.job.tracker", "local");
job.getConfiguration().set("fs.default.name", "local");

FileInputFormat.addInputPaths(job, "output_evideo,conf/dm_common_vid");
FileOutputFormat.setOutputPath(job, new Path("output_vid_evideo"));
job.setMapperClass(EvideoJoinDMVIDFormatMap.class);
job.setReducerClass(EvideoJoinDMVIDFormatReduce.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}

	
	
}
