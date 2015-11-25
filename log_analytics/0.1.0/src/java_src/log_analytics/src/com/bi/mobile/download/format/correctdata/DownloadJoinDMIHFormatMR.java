package com.bi.mobile.download.format.correctdata;

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
import com.bi.mobile.download.format.dataenum.DownloadOutIHFormatEnum;


public class DownloadJoinDMIHFormatMR {

	public static class DownloadJoinDMIHFormatMap extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException,
				UnsupportedEncodingException {
			try {
				String line = value.toString();
				String[] fields= line.split("\t");
                ////System.out.println("fiedls length"+fields.length);
				// dm_inforhash
				if (fields.length == DMInforHashEnum.CHANNEL_ID.ordinal() + 1) {
					String inforhashlinebycomma =line.replaceAll("\t", ",");
					////System.out.println("dm_ihforhash:"+inforhashlinebycomma);
					context.write(
							new Text(fields[DMInforHashEnum.IH.ordinal()].trim().toLowerCase()),
							new Text(inforhashlinebycomma.toLowerCase()));
				}
				// download
				else {
					String inforhash = fields[DownloadOutIHFormatEnum.IH.ordinal()].toLowerCase();
					context.write(new Text(inforhash.trim().toLowerCase()), new Text(line));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
	}

	public static class DownloadJoinDMIHFormatReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// //System.out.println("*********************************************");
			String inforhashStr = null;
			List<String> downloadInfoList = new ArrayList<String>();
			int count = 0; 
			for (Text val : values) {
				String value = val.toString().trim();
				if (value.contains(",")) {
					inforhashStr = value;
					////System.out.println("reduce inforhashStr:"+inforhashStr);
				} else {
					////System.out.println("reduce download info:"+value);
					downloadInfoList.add(value);
				count++;
			}
			}
			////System.out.println("value count:"+count);
			if(count>1){
				//System.out.println("value count:"+count);
			}
			for (String downloadInfoString : downloadInfoList) {
				String downLoadETLValue = "";
				String[] splitDownloadSts = downloadInfoString.split("\t");
				List<String> splitDownloadList = new ArrayList<String>();
				for (String splitDownload : splitDownloadSts) {

					splitDownloadList.add(splitDownload);
				}
				////System.out.println("reduce download size:"+splitDownloadList.size());
				if (null != inforhashStr) {
					String[] inforhashStrs = inforhashStr.split(",");
					//System.out.println("reduce inofrhash :"+inforhashStrs.length);
					splitDownloadList
							.add(DownloadOutIHFormatEnum.CITY_ID.ordinal(),
									inforhashStrs[DMInforHashEnum.CHANNEL_ID
											.ordinal()]);
					splitDownloadList.add(
							DownloadOutIHFormatEnum.PROVINCE_ID.ordinal() + 1,
							inforhashStrs[DMInforHashEnum.MEIDA_ID.ordinal()]);
					splitDownloadList.add(
							DownloadOutIHFormatEnum.PROVINCE_ID.ordinal() + 2,
							inforhashStrs[DMInforHashEnum.SERIAL_ID.ordinal()]);

				} else {
				
					splitDownloadList.add(
							DownloadOutIHFormatEnum.CITY_ID.ordinal(), "-1");
					splitDownloadList.add(
							DownloadOutIHFormatEnum.PROVINCE_ID.ordinal() + 1,
							"-1");
					splitDownloadList.add(
							DownloadOutIHFormatEnum.PROVINCE_ID.ordinal() + 2,
							"-1");
					
				}
				for (int i = 0; i < splitDownloadList.size(); i++) {
					////System.out.println("splitDownloadList "+i+" value "+splitDownloadList.get(i));
					downLoadETLValue +=splitDownloadList.get(i);
					if (i < splitDownloadList.size()) {
						downLoadETLValue +="\t";
					}
				}
				////System.out.println(downLoadETLValue);
				context.write(new Text(downLoadETLValue),new Text("")
						);
			}

		}
	}
	


	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(DownloadJoinDMIHFormatMR.class);
		job.setJobName("DownloadJoinDMIHETLMR");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");

		FileInputFormat.addInputPath(job, new Path("output_download_outih"));
		FileOutputFormat.setOutputPath(job, new Path("output_download_new"));
		job.setMapperClass(DownloadJoinDMIHFormatMap.class);
		job.setReducerClass(DownloadJoinDMIHFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
