package com.bi.client.fsplayafter.format.correctdata;

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

import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.DMInforHashEnum;
import com.bi.client.fsplayafter.format.correctdata.FsplayAfterOutIHFormatMR.FsplayAfterOutIHFormatMap;
import com.bi.client.fsplayafter.format.correctdata.FsplayAfterOutIHFormatMR.FsplayAfterOutIHFormatReduce;
import com.bi.client.fsplayafter.format.dataenum.FsplayAfterOtherETLEnum;

public class FsplayAfterJoinDMIHFormatMR {

	public static class FsplayAfterJoinDMIHFormatMap extends
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
				// FsplayAfter
				else {
					String inforhash = fields[FsplayAfterOtherETLEnum.IH.ordinal()];
					context.write(new Text(inforhash.trim().toLowerCase()),
							new Text(line));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
	}

	public static class FsplayAfterJoinDMIHFormatReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String inforhashStr = null;
			List<String> FsplayAfterInfoList = new ArrayList<String>();
			for (Text val : values) {
				String value = val.toString().trim();
				if (value.contains(",")) {
					inforhashStr = value;
					
				} else {

					FsplayAfterInfoList.add(value);
				}
			}
			for (String FsplayAfterInfoString : FsplayAfterInfoList) {
				String FsplayAfterETLValue = "";
				String[] splitDownloadSts = FsplayAfterInfoString.split("\t");
				List<String> splitFsplayAfterList = new ArrayList<String>();
				for (String splitDownload : splitDownloadSts) {

					splitFsplayAfterList.add(splitDownload);
				}

				if (null != inforhashStr) {
					String[] inforhashStrs = inforhashStr.split(",");

					splitFsplayAfterList
							.add(FsplayAfterOtherETLEnum.CITY_ID.ordinal(),
									inforhashStrs[DMInforHashEnum.CHANNEL_ID
											.ordinal()]);
					splitFsplayAfterList.add(
							FsplayAfterOtherETLEnum.PROVINCE_ID.ordinal() + 1,
							inforhashStrs[DMInforHashEnum.MEIDA_ID.ordinal()]);
					splitFsplayAfterList.add(
							FsplayAfterOtherETLEnum.PROVINCE_ID.ordinal() + 2,
							inforhashStrs[DMInforHashEnum.SERIAL_ID.ordinal()]);

				} else {

					splitFsplayAfterList.add(FsplayAfterOtherETLEnum.CITY_ID.ordinal(),
							"-1");
					splitFsplayAfterList
							.add(FsplayAfterOtherETLEnum.PROVINCE_ID.ordinal() + 1,
									"-1");
					splitFsplayAfterList
							.add(FsplayAfterOtherETLEnum.PROVINCE_ID.ordinal() + 2,
									"-1");

				}
				for (int i = 0; i < splitFsplayAfterList.size(); i++) {

					FsplayAfterETLValue += splitFsplayAfterList.get(i);
					if (i < splitFsplayAfterList.size()) {
						FsplayAfterETLValue += "\t";
					}
				}

				context.write(new Text(FsplayAfterETLValue), new Text(""));
			}

		}
	}
	
	
	public static void main(String[] args) throws IOException,
	InterruptedException, ClassNotFoundException {
// TODO Auto-generated method stub
Job job = new Job();
job.setJarByClass(FsplayAfterJoinDMIHFormatMR.class);
job.setJobName("FsplayAfterJoinDMIHFormatETL");
job.getConfiguration().set("mapred.job.tracker", "local");
job.getConfiguration().set("fs.default.name", "local");

FileInputFormat.addInputPaths(job, "output_fsplayafter,conf/dm_common_infohash_client");
FileOutputFormat.setOutputPath(job, new Path("output_ih_fsplayafter"));
job.setMapperClass(FsplayAfterJoinDMIHFormatMap.class);
job.setReducerClass(FsplayAfterJoinDMIHFormatReduce.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}

	
	

}
