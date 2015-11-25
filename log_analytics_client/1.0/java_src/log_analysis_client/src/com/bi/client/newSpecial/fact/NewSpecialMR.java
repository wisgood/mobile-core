/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: NewSpecialChannelMR.java 
* @Package com.bi.client.newSpecial.fact 
* @Description:  pgclick日志计算新用户首页的 分版本渠道vv及vv的mac排重、计算分媒体类型媒体ID渠道的vv及vv的mac排重数
* @author limm
* @date 2013-9-12 下午3:17:19 
* @input:输入日志路径/dw/logs/client/format/pgclick
* @output:输出日志路径/dw/logs/3_client/4_newSpecial/
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.client.newSpecial.fact;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.client.pgclick.format.PgclickLogFormatEnum;
import com.bi.client.util.ChannelIdInfo;

public class NewSpecialMR extends Configured implements Tool{
	private static final String NEWSPECIAL_FLAG = "fs.funshion.com/newspecial";
	private static final String FOCUS_FLAG = "slider";
	private static final String LONGVIDEO_PLAY_FLAG = "fsp:";
	private static final String MICROVIDEO_PLAY_FLAG = "fs.funshion.com/video/play";
	private static final String LONG_VIDEO = "1";
	private static final String MICRO_VIDEO = "2";
	private static final String ALL_VIDEO = "0";
	private static final String ALL_MEDIAID = "0";
	private static final String ALL_VERSIONID = "-1";
	
	public static class NewSpecialFirstMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String ext = null;
			String mediaId = null;
			String provinceId = null;
			String hourId = null;
			String versionId = null;
			String videoType = null;
			String channelId = null;
			String mac= null;
			String newSpecial_play_flag = "1";
			String[] fields = value.toString().split("\t");
			String block = fields[PgclickLogFormatEnum.BLOCK.ordinal()];
			String url = fields[PgclickLogFormatEnum.URL.ordinal()];
			if(url.contains(NEWSPECIAL_FLAG) && !block.contains(FOCUS_FLAG)){   //url为新用户首页，且block不为焦点图
				ext = fields[PgclickLogFormatEnum.EXT.ordinal()];
				if(ext.contains(LONGVIDEO_PLAY_FLAG)){
					videoType = LONG_VIDEO;
					String[] strs = ext.split("\\|");
					if(strs.length >= 4 && strs[4].matches("^m=\\d+$")){
						String[] mediaInfo = strs[4].split("=");
						mediaId = mediaInfo[1];
					}else{
						newSpecial_play_flag = "0";
					}
				}else if(ext.contains(MICROVIDEO_PLAY_FLAG)){
					videoType = MICRO_VIDEO;
					String[] strs = ext.split("/");
					mediaId = strs[5];
				}else{
					newSpecial_play_flag = "0";
				}
		
				if(newSpecial_play_flag.equals("1")){
					hourId = fields[PgclickLogFormatEnum.HOUR_ID.ordinal()];
					provinceId = fields[PgclickLogFormatEnum.PROVINCE_ID.ordinal()];
					versionId = fields[PgclickLogFormatEnum.VERSION_ID.ordinal()];
					mac = fields[PgclickLogFormatEnum.MAC.ordinal()];
					channelId = fields[PgclickLogFormatEnum.CHANNEL_ID.ordinal()];
					if(mediaId == null || !mediaId.matches("^\\d+$")){
						mediaId = "0";
					}
					newKey.set(mac);
					newValue.set(hourId + "," + provinceId + "," + versionId + "," +  channelId + "," + videoType + "," + mediaId);
					context.write(newKey, newValue); 
				}
			}
		}
	}

	public static class NewSpecialFirstReducer extends
		Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		private HashMap<String, Integer> dimMap = new HashMap<String, Integer>();
	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			dimMap.clear();
			String dim = null;
			for(Text val : values){
				dim = val.toString();
				if(dimMap.containsKey(dim)){
					dimMap.put(dim, dimMap.get(dim)+1);
				}else{
					dimMap.put(dim, 1);
				}
			}
			StringBuilder dimSB = new StringBuilder();
			for(String mapKey : dimMap.keySet()){
				if(dimSB.length() > 0){
					dimSB.append("\t");
				}
				dimSB.append(mapKey).append(",").append(dimMap.get(mapKey));
			}
			newValue.set(dimSB.toString());
		context.write(key, newValue);
		}
	}
	
	public static class NewSpecialSecondMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();
		private ChannelIdInfo channelIdInfo = null;
		private HashMap<String, String> channelIdMap = new HashMap<String, String>();
		private HashMap<String, Integer> dimVersionChannelIdMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> dimMediaVideotypeChannelIdMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> dimMediaProvinceIdMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> dimMediaHourIdMap = new HashMap<String, Integer>();
		
		@Override
		public void setup(Context context) throws IOException{
			channelIdInfo = new ChannelIdInfo();
			try {
				channelIdInfo.init("channelId_info");
				channelIdMap = channelIdInfo.getChannelIdMap();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(0);
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			dimVersionChannelIdMap.clear();
			dimMediaVideotypeChannelIdMap.clear();
			dimMediaProvinceIdMap.clear();
			dimMediaHourIdMap.clear();
			String hourId = null;
			String provinceId = null;
			String mediaId = null;
			String versionId = null;
			String videoType = null;
			String channelId = null;
			String tmpChannelId = null;

			int num = 0;
			String[] fields = value.toString().split("\t");
			for(int i=1; i<fields.length; i++){			
				String[] strs = fields[i].split(",");
				hourId = strs[0];
				provinceId = strs[1];
				versionId = strs[2];
				channelId = strs[3];
				videoType = strs[4];
				mediaId = strs[5];
				num = Integer.parseInt(strs[6]);
				tmpChannelId = channelId;
				List<String> versionChannelList = new ArrayList<String>();
				List<String> mediaVideotypeChannelList = new ArrayList<String>();
				while(channelIdMap.containsKey(tmpChannelId)){
					versionChannelList.add(versionId + "\t" + tmpChannelId);
					versionChannelList.add(ALL_VERSIONID + "\t" + tmpChannelId);
					mediaVideotypeChannelList.add(tmpChannelId + "\t" + mediaId + "\t" + videoType);
					mediaVideotypeChannelList.add(tmpChannelId + "\t" + ALL_MEDIAID + "\t" + videoType);
					mediaVideotypeChannelList.add(tmpChannelId + "\t" + ALL_MEDIAID + "\t" + ALL_VIDEO);
					tmpChannelId = channelIdMap.get(tmpChannelId);
				}
				for(int j=0; j<versionChannelList.size(); j++){
					String mapKey = versionChannelList.get(j);
					if(dimVersionChannelIdMap.containsKey(mapKey)){
						dimVersionChannelIdMap.put(mapKey, num + dimVersionChannelIdMap.get(mapKey));
					}else{
						dimVersionChannelIdMap.put(mapKey, num);
					}
				}
				for(int j=0 ; j<mediaVideotypeChannelList.size(); j++){
					String mapKey = mediaVideotypeChannelList.get(j);
					if(dimMediaVideotypeChannelIdMap.containsKey(mapKey)){
						dimMediaVideotypeChannelIdMap.put(mapKey, num + dimMediaVideotypeChannelIdMap.get(mapKey));
					}else{
						dimMediaVideotypeChannelIdMap.put(mapKey, num);
					}
				}
				if(!videoType.equals(LONG_VIDEO)){
					continue;
				}
				List<String> mediaHourList = new ArrayList<String>();
				List<String> mediaProvinceList = new ArrayList<String>();
				mediaHourList.add(hourId + "\t" + mediaId);
				mediaHourList.add(hourId + "\t" + ALL_MEDIAID);
				mediaProvinceList.add(provinceId + "\t" + mediaId);
				mediaProvinceList.add(provinceId + "\t" + ALL_MEDIAID);
				for(int j=0; j<mediaHourList.size(); j++){
					String mapKey = mediaHourList.get(j);
					if(dimMediaHourIdMap.containsKey(mapKey)){
						dimMediaHourIdMap.put(mapKey, num + dimMediaHourIdMap.get(mapKey));
					}else{
						dimMediaHourIdMap.put(mapKey, num);
					}
				}
				for(int j=0; j<mediaProvinceList.size(); j++){
					String mapKey = mediaProvinceList.get(j);
					if(dimMediaProvinceIdMap.containsKey(mapKey)){
						dimMediaProvinceIdMap.put(mapKey, num + dimMediaProvinceIdMap.get(mapKey));
					}else{
						dimMediaProvinceIdMap.put(mapKey, num);
					}
				}
			}
			for(String mapKey : dimVersionChannelIdMap.keySet()){
				newKey.set("v" + mapKey);
				newValue.set(dimVersionChannelIdMap.get(mapKey) + "," + "1");
				context.write(newKey, newValue);
			}
			for(String mapKey : dimMediaVideotypeChannelIdMap.keySet()){
				newKey.set("m" + mapKey);
				newValue.set(dimMediaVideotypeChannelIdMap.get(mapKey) + "," + "1");
				context.write(newKey, newValue);
			}
			for(String mapKey : dimMediaProvinceIdMap.keySet()){
				newKey.set("p" + mapKey);
				newValue.set(dimMediaProvinceIdMap.get(mapKey) + "," + "1");
				context.write(newKey, newValue);
			}
			for(String mapKey : dimMediaHourIdMap.keySet()){
				newKey.set("h" + mapKey);
				newValue.set(dimMediaHourIdMap.get(mapKey) + "," + "1");
				context.write(newKey, newValue);
			}
		}
	}
	
	public static class NewSpecialSecondCombiner extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			long videoViewNum = 0;
			long videoViewUserNum = 0;
			for(Text val : values){
				String[] fields = val.toString().split(",");
				videoViewNum += Long.parseLong(fields[0]);
				videoViewUserNum += Long.parseLong(fields[1]);
			}
			newValue.set(videoViewNum + "," + videoViewUserNum);
			context.write(key, newValue);
		}
	}

	public static class NewSpecialSecondReduce extends
			Reducer<Text, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();
		private String date = null;
		private MultipleOutputs<Text, Text> multipleOutputs = null;

		@Override
		public void setup(Context context){
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
			date = context.getConfiguration().get("stat_date");
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			long videoViewNum = 0;
			long videoViewUserNum = 0;
			for(Text val : values){
				String[] fields = val.toString().split(",");
				videoViewNum += Long.parseLong(fields[0]);
				videoViewUserNum += Long.parseLong(fields[1]);
			}
			String flag = key.toString().substring(0,1);
			String outputDir = "";
			if(flag.equals("p")){
				outputDir = "F_CLIENT_SP_VV_PROVINCE"; 			
				newValue.set(Long.toString(videoViewNum));
			}else if(flag.equals("h")){
				outputDir = "F_CLIENT_SP_VV_HOUR";
				newValue.set(Long.toString(videoViewNum));
			}else if(flag.equals("v")){
				outputDir = "F_CLIENT_SP_VV_VERSION";
				newValue.set(videoViewNum + "\t" + videoViewUserNum);
			}else{
				outputDir = "F_CLIENT_SP_VV_CHANNEL";
				newValue.set(videoViewNum + "\t" + videoViewUserNum);
			}
			
			newKey.set(date + "\t" + key.toString().substring(1));
			multipleOutputs.write(newKey, newValue, outputDir);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			multipleOutputs.close();
		}
	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();

		Job job = new Job(conf, "ClientNewSpecialMR");
		job.setJarByClass(NewSpecialMR.class);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		String outputDir = conf.get("output_dir");
		String tmpDir = outputDir + "_tmp";
		Path tmpOutput = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, tmpOutput);
		tmpOutput.getFileSystem(conf).delete(tmpOutput, true);

		job.setMapperClass(NewSpecialFirstMapper.class);
		job.setReducerClass(NewSpecialFirstReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(20);

		int code = job.waitForCompletion(true) ? 0 : 1;

		if(code == 0){
			Job secondJob = new Job(conf, "NewSpecialResult");
			secondJob.setJarByClass(NewSpecialMR.class);
			conf.set("stat_date", conf.get("stat_date"));
	
			FileInputFormat.addInputPath(secondJob, new Path(tmpDir));
			Path output = new Path(outputDir);
			FileOutputFormat.setOutputPath(secondJob, output);
			output.getFileSystem(conf).delete(output, true);
	
			secondJob.setMapperClass(NewSpecialSecondMapper.class);
			secondJob.setCombinerClass(NewSpecialSecondCombiner.class);
			secondJob.setReducerClass(NewSpecialSecondReduce.class);
	
			secondJob.setInputFormatClass(TextInputFormat.class);
			secondJob.setOutputFormatClass(TextOutputFormat.class);
			secondJob.setOutputKeyClass(Text.class);
			secondJob.setOutputValueClass(Text.class);
		
			secondJob.setNumReduceTasks(1);

			code = secondJob.waitForCompletion(true)? 0 : 1;
		}
		FileSystem.get(conf).delete(tmpOutput, true);
		System.exit(code);
		return code;
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new NewSpecialMR(), args);
		System.out.println(res);
	}
}
