package com.bi.client.fact.month;

import java.io.IOException;
import java.util.HashMap;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.bi.client.taskstat.format.TaskstatLogFormatEnum;

public class ClientUserUIOnlineTimeMR extends Configured implements Tool{
	
	public static class UIOnlineTimeFirstMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		
		private static final int MACCOLNUM = TaskstatLogFormatEnum.MAC.ordinal();
		private static final int PROVINCECOLNUM = TaskstatLogFormatEnum.PROVINCE_ID.ordinal();   
		private static final int VERSIONCOLNUM = TaskstatLogFormatEnum.VERSION_ID.ordinal();
		private static final int ISPCOLNUM = TaskstatLogFormatEnum.ISP_ID.ordinal();
		private static final int PVSCOLNUM = TaskstatLogFormatEnum.PVS.ordinal();
		private Text newKey = new Text();
		private Text newValue = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] fields = value.toString().split("\t");
			String provinceId = fields[PROVINCECOLNUM];
			String versionId = fields[VERSIONCOLNUM];
			String ispId = fields[ISPCOLNUM];
			String mac = fields[MACCOLNUM];
			String pvs = fields[PVSCOLNUM];
			if(pvs.equals("2")){
				newKey.set(mac);
				newValue.set(provinceId + "," + ispId + "," + versionId);
				context.write(newKey, newValue);
			}
		}
	}
	
	public static class UIOnlineTimeFirstReduce extends
			Reducer<Text, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();
		private String date = null;
		private HashMap<String, Integer> provinceMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> ispMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> versionMap = new HashMap<String, Integer>();
		private static Logger logger = Logger.getLogger(ClientUserUIOnlineTimeMR.class.getName());
		
		@Override
		public void setup(Context context){
			date = context.getConfiguration().get("stat_date");
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			provinceMap.clear();
			ispMap.clear();
			versionMap.clear();
			String provinceId = null;
			String ispId = null;
			String versionId = null;
			int dateTotalNum = 0;
			
			try {
				for(Text val : values){
					String[] fields = val.toString().split(",");
					provinceId = fields[0];
					ispId = fields[1];
					versionId = fields[2];
					
					dateTotalNum += 1;
					
					if(provinceMap.containsKey(provinceId)){
						provinceMap.put(provinceId, provinceMap.get(provinceId) + 1);
					}else{
						provinceMap.put(provinceId, 1);
					}
					
					if(ispMap.containsKey(ispId)){
						ispMap.put(ispId, ispMap.get(ispId) + 1);
					}else{
						ispMap.put(ispId, 1);
					}
					
					if(versionMap.containsKey(versionId)){
						versionMap.put(versionId, versionMap.get(versionId) + 1);
					}else{
						versionMap.put(versionId, 1);
					}
				}
				
				newKey.set("d" + date);
				newValue.set(dateTotalNum + "");
				context.write(newKey, newValue);
				for(String dimKey : provinceMap.keySet()){
					newKey.set("p" + dimKey);
					newValue.set(provinceMap.get(dimKey) + "");
					context.write(newKey, newValue);
				}
				for(String dimKey : ispMap.keySet()){
					newKey.set("i" + dimKey);
					newValue.set(ispMap.get(dimKey) + "");
					context.write(newKey, newValue);
				}
				for(String dimKey : versionMap.keySet()){
					newKey.set("v" + dimKey);
					newValue.set(versionMap.get(dimKey) + "");
					context.write(newKey, newValue);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("error originalData: " + key.toString() + "\t" + newValue.toString());
			}
		}
	}
	
	public static class UIOnlineTimeSecondMapper extends
			Mapper<Text, Text, Text, Text>{
		@Override
		public void map(Text key, Text value, Context context) 
				throws IOException, InterruptedException{
			context.write(key, value);
		}
	} 
	
	public static class UIOnlineTimeSecondCombiner extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			long totalCount = 0;
			long userCount = 0;
			for(Text val : values){
				totalCount += Integer.parseInt(val.toString());
				userCount += 1;
			}
			newValue.set(totalCount + "," + userCount);
			context.write(key, newValue);
		}
	}
	
	public static class UIOnlineTimeSecondReduce extends
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
			long totalCount = 0;
			long userCount = 0;
			for(Text val : values){
				String[] nums = val.toString().split(",");
				totalCount += Integer.parseInt(nums[0]);
				userCount += Integer.parseInt(nums[1]);
			}
			double averageUiOnlineTime = (double)(totalCount*180)/userCount;
			
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
				newKey.set(key.toString().substring(1));
			}else{
				newKey.set(date + "\t" + key.toString().substring(1));
			}

			multipleOutputs.write(newKey, new Text("16" + "\t" + averageUiOnlineTime), outputDir);
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
		
		Job job = new Job(conf, "UIOnlineTimeMR");
		job.setJarByClass(ClientUserUIOnlineTimeMR.class);
		conf.set("stat_date", conf.get("stat_date"));
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		String outputDir = conf.get("output_dir");
		String tmpDir = outputDir + "_tmp";
		Path tmpOutput = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, tmpOutput);
		tmpOutput.getFileSystem(conf).delete(tmpOutput, true);
		
		job.setMapperClass(UIOnlineTimeFirstMapper.class);
		job.setReducerClass(UIOnlineTimeFirstReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(20);
		
		int code = job.waitForCompletion(true) ? 0 : 1;
		
		if(code == 0){
			Job secondJob = new Job(conf, "UIOnlineTimeResult");
			secondJob.setJarByClass(ClientUserUIOnlineTimeMR.class);
			conf.set("stat_date", conf.get("stat_date"));
			
			FileInputFormat.addInputPath(secondJob, new Path(tmpDir));
			Path output = new Path(outputDir);
			FileOutputFormat.setOutputPath(secondJob, output);
			output.getFileSystem(conf).delete(output, true);
			
			secondJob.setMapperClass(UIOnlineTimeSecondMapper.class);
			secondJob.setCombinerClass(UIOnlineTimeSecondCombiner.class);
			secondJob.setReducerClass(UIOnlineTimeSecondReduce.class);
			
			secondJob.setInputFormatClass(KeyValueTextInputFormat.class);
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
		int res = ToolRunner.run(new Configuration(), new ClientUserUIOnlineTimeMR(), args);
		System.out.println(res);
	}
}
