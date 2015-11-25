package com.bi.client.fact.month;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.client.wtbh.format.WtbhLogFormatEnum;

public class ClientUserOnlinePlayTimeMR extends Configured implements Tool{
	
	public static class ClientUserOnlinePlayTimeFirstMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();
		private String pathName = null;
		
		@Override
		public void setup(Context context){
			pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			if(Pattern.matches(".*/wtbh/.*", pathName)){
				String[] fields = value.toString().split("\t");
				String versionId = fields[WtbhLogFormatEnum.VERSION_ID.ordinal()];
				String provinceId = fields[WtbhLogFormatEnum.PROVINCE_ID.ordinal()];
				String mac = fields[WtbhLogFormatEnum.MAC.ordinal()];
				String ispId = fields[WtbhLogFormatEnum.ISP_ID.ordinal()];
				String playTime = fields[WtbhLogFormatEnum.PLAY_TIME.ordinal()];
				String stopTime = fields[WtbhLogFormatEnum.PSTM.ordinal()];
				int pTime = Integer.parseInt(playTime) - Integer.parseInt(stopTime);
				
				if(pTime < 0){
					pTime = 0;
				}else if(pTime > 86400){
					pTime = 86400;
				}
				
				newKey.set(mac);
				newValue.set(provinceId + "," + ispId + "," + versionId + "," + pTime);
			}else{
				String[] fields = value.toString().split("\t");
				String mac = fields[0];
				newKey.set(mac);
				newValue.set(fields[1]);
			}
			context.write(newKey, newValue);
		}
	}
	
	public static class ClientUserOnlinePlayTimeFirstReduce extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		private HashMap<String, Integer> dimOnlineTimeMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> dimPlayTimeMap = new HashMap<String, Integer>();
		private Set<String> dimSet = new HashSet<String>();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			dimOnlineTimeMap.clear();
			dimPlayTimeMap.clear();
			dimSet.clear();
			String provinceId = null;
			String ispId = null;
			String versionId = null;
			String dim = null;
			int onlineTime = 0;
			int playTime = 0;
			int newUserFlag = 0;
			String onlineUserFlag = "0";
			String playUserFlag = "0";
			
			for(Text val : values){
				String[] fields = val.toString().split(",");
				if(fields.length != 4){                           //hs_log
					onlineUserFlag = "1";
					newUserFlag = Integer.parseInt(fields[0]);
					for(int i = 1; i < fields.length; i += 5){
						provinceId = fields[i];
						ispId = fields[i+1];
						versionId = fields[i+2];
						onlineTime = Integer.parseInt(fields[i+3]);
						dim = provinceId + "," + ispId + "," + versionId;
						
						dimSet.add(dim);
						if(dimOnlineTimeMap.containsKey(dim)){
							dimOnlineTimeMap.put(dim, dimOnlineTimeMap.get(dim) + onlineTime);
						}else{
							dimOnlineTimeMap.put(dim, onlineTime);
						}
					}
				}else{                                    //wt_bh
					playUserFlag = "1";
					provinceId = fields[0];
					ispId = fields[1];
					versionId = fields[2];
					playTime = Integer.parseInt(fields[3]);
					dim = provinceId + "," + ispId + "," + versionId;
					
					dimSet.add(dim);
					if(dimPlayTimeMap.containsKey(dim)){
						dimPlayTimeMap.put(dim, dimPlayTimeMap.get(dim) + playTime);
					}else{
						dimPlayTimeMap.put(dim, playTime);
					}
				}
			}
			
			//dim
			String mapKey = null;
			StringBuilder outSB = new StringBuilder();
			outSB.append(newUserFlag).append(",").append(onlineUserFlag).append(",").append(playUserFlag);
			for(Iterator<String> iterator = dimSet.iterator(); iterator.hasNext();){
				mapKey = iterator.next();
				if(dimOnlineTimeMap.containsKey(mapKey)){
					outSB.append(",").append(mapKey).append(",").append(dimOnlineTimeMap.get(mapKey));
				}else{
					outSB.append(",").append(mapKey).append(",").append(0);
				}
				if(dimPlayTimeMap.containsKey(mapKey)){
					outSB.append(",").append(dimPlayTimeMap.get(mapKey));
				}else{
					outSB.append(",").append(0);
				}
			}
				newValue.set(outSB.toString());
				context.write(key, newValue);
		}
	}
	
	public static class ClientUserOnlinePlayTimeSecondMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newValue = new Text();
		private Text newKey = new Text();
		private HashMap<String, Integer> provinceOnlineTimeMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> ispOnlineTimeMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> versionOnlineTimeMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> provincePlayTimeMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> ispPlayTimeMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> versionPlayTimeMap = new HashMap<String, Integer>();
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			provinceOnlineTimeMap.clear();
			ispOnlineTimeMap.clear();
			versionOnlineTimeMap.clear();
			provincePlayTimeMap.clear();
			ispPlayTimeMap.clear();
			versionPlayTimeMap.clear();
			String provinceId = null;
			String ispId = null;
			String versionId = null;
			int onlineTime = 0;
			int playTime = 0;
			int dateOnlineTime = 0;
			int datePlayTime = 0;
			
			String[] fields = value.toString().split("\t");
			String[] strs = fields[1].split(",");
			String newUserFlag = strs[0];
			String onlineUserFlag = strs[1];
			String playUserFlag = strs[2];
			for(int i = 3; i< strs.length; i += 5){
				provinceId = strs[i];
				ispId = strs[i+1];
				versionId = strs[i+2];
				onlineTime = Integer.parseInt(strs[i+3]);
				playTime = Integer.parseInt(strs[i+4]);
				
				dateOnlineTime += onlineTime;
				datePlayTime += playTime;
				if(provinceOnlineTimeMap.containsKey(provinceId)){
					provinceOnlineTimeMap.put(provinceId, provinceOnlineTimeMap.get(provinceId) + onlineTime);
					provincePlayTimeMap.put(provinceId, provincePlayTimeMap.get(provinceId) + playTime);
				}else{
					provinceOnlineTimeMap.put(provinceId, onlineTime);
					provincePlayTimeMap.put(provinceId, playTime);
				}
				if(ispOnlineTimeMap.containsKey(ispId)){
					ispOnlineTimeMap.put(ispId, ispOnlineTimeMap.get(ispId) + onlineTime);
					ispPlayTimeMap.put(ispId, ispPlayTimeMap.get(ispId) + playTime);
				}else{
					ispOnlineTimeMap.put(ispId, onlineTime);
					ispPlayTimeMap.put(ispId, playTime);
				}
				if(versionOnlineTimeMap.containsKey(versionId)){
					versionOnlineTimeMap.put(versionId, versionOnlineTimeMap.get(versionId) + onlineTime);
					versionPlayTimeMap.put(versionId, versionPlayTimeMap.get(versionId) + playTime);
				}else{
					versionOnlineTimeMap.put(versionId, onlineTime);
					versionPlayTimeMap.put(versionId, playTime);
				}
			}
			
			//dim : province
			for(String province : provinceOnlineTimeMap.keySet()){
				newKey.set("p" + province);
				newValue.set(provinceOnlineTimeMap.get(province) + "," + onlineUserFlag + "," + provincePlayTimeMap.get(province) + "," + playUserFlag + "," + newUserFlag);
				context.write(newKey, newValue);
			}
			//dim : isp
			for(String isp : ispOnlineTimeMap.keySet()){
				newKey.set("i" + isp);
				newValue.set(ispOnlineTimeMap.get(isp) + "," + onlineUserFlag + "," + ispPlayTimeMap.get(isp) + "," + playUserFlag + "," + newUserFlag);
				context.write(newKey, newValue);
			}
			//dim : version
			for(String version : versionOnlineTimeMap.keySet()){
				newKey.set("v" + version);
				newValue.set(versionOnlineTimeMap.get(version) + "," + onlineUserFlag + "," + versionPlayTimeMap.get(version) + "," + playUserFlag + "," + newUserFlag);
				context.write(newKey, newValue);
			}
			//dim : date
			newKey.set("d");
			newValue.set(dateOnlineTime + "," + onlineUserFlag + "," + datePlayTime + "," + playUserFlag + "," + newUserFlag);
			context.write(newKey, newValue);
		}
	}
	
	public static class ClientUserOnlinePlayTimeSecondCombiner extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			int onlineUserNum = 0;
			long onlineTime = 0;
			int newOnlineUserNum = 0;
			long playTime = 0;
			int playUserNum = 0;
			
			for(Text val : values){
				String[] fields = val.toString().split(",");
				onlineTime += Long.parseLong(fields[0]);
				onlineUserNum += Integer.parseInt(fields[1]);
				playTime += Long.parseLong(fields[2]);
				playUserNum += Integer.parseInt(fields[3]);
				newOnlineUserNum += Integer.parseInt(fields[4]);
			}
			newValue.set(onlineTime + "," + onlineUserNum + "," + playTime + "," + playUserNum + "," + newOnlineUserNum);
			context.write(key, newValue);
		}
	}
	
	public static class ClientUserOnlinePlayTimeSecondReduce extends  
			Reducer<Text, Text, Text, Text>{
		private Text newKey = new Text();
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
			int onlineUserNum = 0;
			double totalOnlineTime = 0;
			double averageOnlineTime = 0;
			int newOnlineUserNum = 0;
			double totalPlayTime = 0;
			int playUserNum = 0;
			double averageOnlineUserTime = 0;
			double averagePlayUserTime = 0;
			
			for(Text val : values){
				String[] fields = val.toString().split(",");
				
				totalOnlineTime += Long.parseLong(fields[0]);
				onlineUserNum += Integer.parseInt(fields[1]);
				totalPlayTime += Long.parseLong(fields[2]);
				playUserNum += Integer.parseInt(fields[3]);
				newOnlineUserNum += Integer.parseInt(fields[4]);
			}
			
			if(onlineUserNum > 0){
				averageOnlineTime = totalOnlineTime/onlineUserNum;
				averageOnlineUserTime = totalPlayTime/onlineUserNum;
			}
			if(playUserNum > 0){
				averagePlayUserTime = totalPlayTime/playUserNum;
			} 
			totalOnlineTime = totalOnlineTime/36000000;          //ÍòÐ¡Ê±
			totalPlayTime = totalPlayTime/36000000;
			
			String flag = key.toString().substring(0, 1);
			String outputDir = "";
			if(flag.equals("p")){
				outputDir = "F_CLIENT_MONTH_DATE_AREA"; 
			}else if(flag.equals("i")){
				outputDir = "F_CLIENT_MONTH_DATE_ISP";
			}else if(flag.equals("v")){
				outputDir = "F_CLIENT_MONTH_DATE_VERSION";
			}else{
				outputDir = "F_CLIENT_MONTH_DATE";
			}
			
			if(flag.equals("d")){
				newKey.set(date);
			}else{
				newKey.set(date + "\t" + key.toString().substring(1));
			}
			
			if(onlineUserNum > 0)
				multipleOutputs.write(newKey, new Text("1" + "\t" + onlineUserNum), outputDir);
			if(totalOnlineTime > 0)
				multipleOutputs.write(newKey, new Text("5" + "\t" + totalOnlineTime), outputDir);
			if(averageOnlineTime > 0)
				multipleOutputs.write(newKey, new Text("6" + "\t" + averageOnlineTime), outputDir);
			if(newOnlineUserNum > 0)
				multipleOutputs.write(newKey, new Text("7" + "\t" + newOnlineUserNum), outputDir);
			if(totalPlayTime > 0)
				multipleOutputs.write(newKey, new Text("17" + "\t" + totalPlayTime), outputDir);
			if(playUserNum > 0)
				multipleOutputs.write(newKey, new Text("20" + "\t" + playUserNum), outputDir);
			if(averageOnlineUserTime > 0)
				multipleOutputs.write(newKey, new Text("21" + "\t" + averageOnlineUserTime), outputDir);
			if(averagePlayUserTime > 0)
				multipleOutputs.write(newKey, new Text("22" + "\t" + averagePlayUserTime), outputDir);
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

		Job job = new Job(conf, "ClientUserOnlinePlayTimeMR");
		job.setJarByClass(ClientUserOnlinePlayTimeMR.class);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		String outputDir = conf.get("output_dir");
		String tmpDir = outputDir + "_tmp";
		Path tmpOutput = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, tmpOutput);
		tmpOutput.getFileSystem(conf).delete(tmpOutput, true);

		job.setMapperClass(ClientUserOnlinePlayTimeFirstMapper.class);
		job.setReducerClass(ClientUserOnlinePlayTimeFirstReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(40);

		int code = job.waitForCompletion(true) ? 0 : 1;

		if(code == 0){
			Job secondJob = new Job(conf, "ClientUserOnlinePlayTimeResult");
			secondJob.setJarByClass(ClientUserOnlinePlayTimeMR.class);
			conf.set("stat_date", conf.get("stat_date"));
	
			FileInputFormat.addInputPath(secondJob, new Path(tmpDir));
			Path output = new Path(outputDir);
			FileOutputFormat.setOutputPath(secondJob, output);
			output.getFileSystem(conf).delete(output, true);
	
			secondJob.setMapperClass(ClientUserOnlinePlayTimeSecondMapper.class);
			secondJob.setCombinerClass(ClientUserOnlinePlayTimeSecondCombiner.class);
			secondJob.setReducerClass(ClientUserOnlinePlayTimeSecondReduce.class);
	
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
		int res = ToolRunner.run(new Configuration(), new ClientUserOnlinePlayTimeMR(), args);
		System.out.println(res);
	}
}
