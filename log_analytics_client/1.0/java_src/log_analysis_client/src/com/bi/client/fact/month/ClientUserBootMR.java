package com.bi.client.fact.month;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
import org.apache.log4j.Logger;

public class ClientUserBootMR extends Configured implements Tool{
	
	public static class ClientUserBootFirstMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();
		private String date = null;
		
		@Override
		public void setup(Context context){
			date = context.getConfiguration().get("stat_date");
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] fields = value.toString().split("\t");
			String logDate = fields[0];
			String versionId = fields[1];			
			String provinceId = fields[2];
			String mac = fields[3];			
			String ispId = fields[4];
			String bootType = fields[5];
			
			newKey.set(mac);
			newValue.set("d" + date + "," + "p" + provinceId + "," + "i" + ispId + "," + "v" + versionId +  "," + bootType + "," + logDate);
			context.write(newKey, newValue);
		}
	}
	
	public static class ClientUserBootFirstReduce extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();		
		private Set<String> dateBootTypeSet = new HashSet<String>();
		private Set<String> dateBootDateSet = new HashSet<String>();
		private HashMap<String, Set<String>> ispBootTypeMap = new HashMap<String, Set<String>>();
		private HashMap<String, Set<String>> ispBootDateMap = new HashMap<String, Set<String>>();
		private HashMap<String, Set<String>> provinceBootTypeMap = new HashMap<String, Set<String>>();
		private HashMap<String, Set<String>> provinceBootDateMap = new HashMap<String, Set<String>>();
		private HashMap<String, Set<String>> versionBootTypeMap = new HashMap<String, Set<String>>();
		private HashMap<String, Set<String>> versionBootDateMap = new HashMap<String, Set<String>>();
		private static Logger logger = Logger.getLogger(ClientUserBootMR.class.getName());
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			versionBootTypeMap.clear();
			versionBootDateMap.clear();
			ispBootTypeMap.clear();
			ispBootDateMap.clear();
			dateBootTypeSet.clear();
			dateBootDateSet.clear();
			provinceBootTypeMap.clear();
			provinceBootDateMap.clear();
			String date = null;
			
			try {
				for(Text val : values){
					String[] fields = val.toString().split(",");
					date = fields[0];
					String province = fields[1];
					String isp = fields[2];
					String version = fields[3];
					String bootType = fields[4];
					String logDate = fields[5];
					dateBootTypeSet.add(bootType);
					dateBootDateSet.add(logDate);
					if(provinceBootTypeMap.containsKey(province)){
						provinceBootTypeMap.get(province).add(bootType);
						provinceBootDateMap.get(province).add(logDate);
					}else{
						Set<String> bootTypeSet = new HashSet<String>();
						Set<String> bootDateSet = new HashSet<String>();
						bootTypeSet.add(bootType);
						bootDateSet.add(logDate);
						provinceBootTypeMap.put(province,bootTypeSet);
						provinceBootDateMap.put(province, bootDateSet);
					}
					
					if(ispBootTypeMap.containsKey(isp)){
						ispBootTypeMap.get(isp).add(bootType);
						ispBootDateMap.get(isp).add(logDate);
					}else{
						Set<String> bootTypeSet = new HashSet<String>();
						Set<String> bootDateSet = new HashSet<String>();
						bootTypeSet.add(bootType);
						bootDateSet.add(logDate);
						ispBootTypeMap.put(isp,bootTypeSet);
						ispBootDateMap.put(isp, bootDateSet);
					}
					
					if(versionBootTypeMap.containsKey(version)){
						versionBootTypeMap.get(version).add(bootType);
						versionBootDateMap.get(version).add(logDate);
					}else{
						Set<String> bootTypeSet = new HashSet<String>();
						Set<String> bootDateSet = new HashSet<String>();
						bootTypeSet.add(bootType);
						bootDateSet.add(logDate);
						versionBootTypeMap.put(version,bootTypeSet);
						versionBootDateMap.put(version, bootDateSet);
					}
				}
				
				StringBuilder dateSB = new StringBuilder();
				for(Iterator<String> iterator = dateBootTypeSet.iterator(); iterator.hasNext();){
					if(dateSB.length() > 0){
						dateSB.append(":");
					}
					dateSB.append(iterator.next());
				}
				context.write(new Text(date), new Text(dateSB.toString() + "," + dateBootDateSet.size() + "," + "1"));
				
				for(String mapKey : provinceBootTypeMap.keySet()){
					Set<String> bootTypeSet = provinceBootTypeMap.get(mapKey);
					StringBuilder bootTypeStr = new StringBuilder();
					for(Iterator<String> iterator = bootTypeSet.iterator(); iterator.hasNext();){
						if(bootTypeStr.length() > 0 ){
							bootTypeStr.append(":");
						}
						bootTypeStr.append(iterator.next());
					}
					context.write(new Text(mapKey), new Text(bootTypeStr.toString() + "," + provinceBootDateMap.get(mapKey).size() + "," + "1"));
				}
				
				for(String mapKey : ispBootTypeMap.keySet()){
					Set<String> bootTypeSet = ispBootTypeMap.get(mapKey);
					StringBuilder bootTypeStr = new StringBuilder();
					for(Iterator<String> iterator = bootTypeSet.iterator(); iterator.hasNext();){
						if(bootTypeStr.length() > 0 ){
							bootTypeStr.append(":");
						}
						bootTypeStr.append(iterator.next());
					}
					context.write(new Text(mapKey), new Text(bootTypeStr.toString() + "," + ispBootDateMap.get(mapKey).size() + "," + "1"));
				}
				
				for(String mapKey : versionBootTypeMap.keySet()){
					Set<String> bootTypeSet = versionBootTypeMap.get(mapKey);
					StringBuilder bootTypeStr = new StringBuilder();
					for(Iterator<String> iterator = bootTypeSet.iterator(); iterator.hasNext();){
						if(bootTypeStr.length() > 0 ){
							bootTypeStr.append(":");
						}
						bootTypeStr.append(iterator.next());
					}
					context.write(new Text(mapKey), new Text(bootTypeStr.toString() + "," + versionBootDateMap.get(mapKey).size() + "," + "1"));
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("error originalData: " + key.toString() + "\t" + newValue.toString());
			}
		}
	}

	public static class ClientUserBootSecondMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] fields = value.toString().split("\t");
			newKey.set(fields[0]);
			newValue.set(fields[1]);
			context.write(newKey, newValue);
		}
	}
	
	public static class ClientUserBootSecondReduce extends  
			Reducer<Text, Text, Text, Text>{
		private Text newKey = new Text();
		private String date = null;
		private String dateNum = null;
		private MultipleOutputs<Text, Text> multipleOutputs = null;

		@Override
		public void setup(Context context){
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
			date = context.getConfiguration().get("stat_date");
			dateNum = context.getConfiguration().get("stat_dateNum");
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			int totalBootDayNum = 0;
			int bootUserNum = 0;
			int handBootUser = 0;
			int autoHideBootUser = 0;
			int autoNohideBootUser = 0;
			int afterBootUser = 0;
			double averageBootDayNum = 0;
			double bootBetweenDayNum = 0;
			HashMap<String, Integer> bootNumMap = new HashMap<String, Integer>();
			
			for(Text val : values){
				String[] strs = val.toString().split(",");
				String[] bootTypes = strs[0].split(":");
				for(int i = 0; i < bootTypes.length; i++){
					String type = bootTypes[i];
					if(bootNumMap.containsKey(type)){
						bootNumMap.put(type, bootNumMap.get(type) + 1);
					}else{
						bootNumMap.put(type, 1);
					}
				}
				
				totalBootDayNum += Integer.parseInt(strs[1]);
				bootUserNum += Integer.parseInt(strs[2]);
			}
			
			if(bootNumMap.containsKey("0")){
				handBootUser = bootNumMap.get("0");
			}
			if(bootNumMap.containsKey("1")){
				autoHideBootUser = bootNumMap.get("1");
			}
			if(bootNumMap.containsKey("2")){
				autoNohideBootUser = bootNumMap.get("2");
			}
			if(bootNumMap.containsKey("3")){
				afterBootUser = bootNumMap.get("3");
			}
			averageBootDayNum = (double)totalBootDayNum/bootUserNum;
			bootBetweenDayNum = Integer.parseInt(dateNum)/averageBootDayNum;
			
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

			multipleOutputs.write(newKey, new Text("8" + "\t" + bootUserNum), outputDir);
			multipleOutputs.write(newKey, new Text("9" + "\t" + handBootUser), outputDir);
			multipleOutputs.write(newKey, new Text("10" + "\t" + autoHideBootUser), outputDir);
			multipleOutputs.write(newKey, new Text("11" + "\t" + autoNohideBootUser), outputDir);
			multipleOutputs.write(newKey, new Text("12" + "\t" + afterBootUser), outputDir);
			multipleOutputs.write(newKey, new Text("18" + "\t" + averageBootDayNum), outputDir);
			multipleOutputs.write(newKey, new Text("19" + "\t" + bootBetweenDayNum), outputDir);
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

		Job job = new Job(conf, "ClientUserBootMR");
		job.setJarByClass(ClientUserBootMR.class);
		conf.set("stat_date", conf.get("stat_date"));
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		String outputDir = conf.get("output_dir");
		String tmpDir = outputDir + "_tmp";
		Path tmpOutput = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, tmpOutput);
		tmpOutput.getFileSystem(conf).delete(tmpOutput, true);

		job.setMapperClass(ClientUserBootFirstMapper.class);
		job.setReducerClass(ClientUserBootFirstReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(30);

		int code = job.waitForCompletion(true) ? 0 : 1;

		if(code == 0){
			Job secondJob = new Job(conf, "ClientUserBootResult");
			secondJob.setJarByClass(ClientUserBootMR.class);
			conf.set("stat_date", conf.get("stat_date"));
			conf.set("stat_dateNum", conf.get("stat_dateNum"));
	
			FileInputFormat.addInputPath(secondJob, new Path(tmpDir));
			Path output = new Path(outputDir);
			FileOutputFormat.setOutputPath(secondJob, output);
			output.getFileSystem(conf).delete(output, true);
	
			secondJob.setMapperClass(ClientUserBootSecondMapper.class);
			secondJob.setReducerClass(ClientUserBootSecondReduce.class);
	
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
		int res = ToolRunner.run(new Configuration(), new ClientUserBootMR(), args);
		System.out.println(res);
	}
}
