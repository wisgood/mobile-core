package com.bi.client.fact.month;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HsLogMacDistinctMR extends Configured implements Tool{
	
	public static class HsLogMacDistinctMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();
		private String pathName = null;
		
		@Override
		public void setup(Context context){
			pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
		}
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			if(Pattern.matches(".*/1_history_mac/.*", pathName)){
				String mac = value.toString();
				newKey.set(mac);
				newValue.set("h");
			}else{
				String[] fields = value.toString().split("\t");
				String mac = fields[HsLogFormatEnum.MAC.ordinal()];
				String provinceId = fields[HsLogFormatEnum.PROVINCE_ID.ordinal()];
				String ispId = fields[HsLogFormatEnum.ISP_ID.ordinal()];
				String versionId = fields[HsLogFormatEnum.VERSION_ID.ordinal()];
				String dateId = fields[HsLogFormatEnum.DATE_ID.ordinal()];
				int downloadFlux = Integer.parseInt(fields[HsLogFormatEnum.DOWNLOADFLUX.ordinal()]);
				String loginTime = fields[HsLogFormatEnum.LOGINTIME.ordinal()];
				String logoutTime = fields[HsLogFormatEnum.LOGOUTTIME.ordinal()];
				
				int onlineTime = Integer.parseInt(logoutTime) - Integer.parseInt(loginTime);
				onlineTime = (onlineTime > 0 ? onlineTime : 0);
				newKey.set(mac);
				newValue.set(dateId + "," + provinceId + "," + ispId + "," + versionId + "," + onlineTime + "," + downloadFlux); 
			}
			context.write(newKey, newValue);
		}
	}
	
	public static class HsLogMacDistinctReduce extends
			Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			
			HashMap<String, Integer> dimOnlineTimeMap = new HashMap<String, Integer>();
			HashMap<String, HashMap<String, Long>> dimDateDownloadMap = new HashMap<String, HashMap<String, Long>>();
			String newUserFlag = "1";
			String provinceId = null;
			String ispId = null;
			String versionId = null;
			String dateId = null;
			long downloadFlux = 0;
			int onlineTime = 0;
			String hsFlag = "0";
			
			for(Text val : values){
				String[] fields = val.toString().split(",");
				if(fields.length == 1){                                                   //history mac
					newUserFlag = "0";
				}else{
					dateId = fields[0];
					provinceId = fields[1];
					ispId = fields[2];
					versionId = fields[3];
					onlineTime = Integer.parseInt(fields[4]);
					downloadFlux = Long.parseLong(fields[5]);
					hsFlag = "1";
					
					String dim = provinceId + "," + ispId + "," + versionId;
					if(dimOnlineTimeMap.containsKey(dim)){
						dimOnlineTimeMap.put(dim, dimOnlineTimeMap.get(dim) + onlineTime);
						HashMap<String, Long> hashmap = dimDateDownloadMap.get(dim);
						if(hashmap.containsKey(dateId)){
							hashmap.put(dateId, hashmap.get(dateId) + downloadFlux);
						}else{
							hashmap.put(dateId, downloadFlux);
						}
						dimDateDownloadMap.put(dim, hashmap);
					}else{
						dimOnlineTimeMap.put(dim, onlineTime);
						HashMap<String, Long> hashmap = new HashMap<String, Long>();
						hashmap.put(dateId, downloadFlux);
						dimDateDownloadMap.put(dim, hashmap);
					}
				}
			}
			
			if(hsFlag.equals("1")){
				HashMap<String, Long> hashmap = new HashMap<String, Long>();
				StringBuilder outSB = new StringBuilder();
				outSB.append(newUserFlag);
				for(String dim : dimOnlineTimeMap.keySet()){
					hashmap = dimDateDownloadMap.get(dim);
					StringBuilder dateDownloadSB = new StringBuilder();
					for(String date : hashmap.keySet()){
						if(dateDownloadSB.length() > 0){
							dateDownloadSB.append(":");
						}
						dateDownloadSB.append(date).append(":").append(hashmap.get(date));
					}
					outSB.append(",").append(dim).append(",").append(dimOnlineTimeMap.get(dim)).append(",").append(dateDownloadSB);
				}
				context.write(key, new Text(outSB.toString()));
			}
		}
	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();
		
		Job job = new Job(conf, "HsLogMacDistinctMR");
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		Path output = new Path(conf.get("output_dir"));
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
		
		job.setJarByClass(HsLogMacDistinctMR.class);
		job.setMapperClass(HsLogMacDistinctMapper.class);
		job.setReducerClass(HsLogMacDistinctReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(50);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int nRet = ToolRunner.run(new Configuration(), new HsLogMacDistinctMR(), args);
		System.out.println(nRet);
	}
}
