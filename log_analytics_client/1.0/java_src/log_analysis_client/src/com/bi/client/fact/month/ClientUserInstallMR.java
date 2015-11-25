package com.bi.client.fact.month;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ClientUserInstallMR extends Configured implements Tool{
	
	public static class ClientUserInstallFirstMapper extends
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
			if(Pattern.matches(".*/install/.*", pathName)){
				String[] fields = value.toString().split("\t");
				String logDate = fields[0];
				String versionId = fields[1];
				String provinceId = fields[2];
				String mac = fields[3];
				String ispId = fields[4];
				
				newKey.set(mac);
				newValue.set(provinceId + "," + ispId + "," + versionId + "," + logDate);
			}else{
				String[] fields = value.toString().split("\t");
				String mac = fields[0];
				newKey.set(mac);
				newValue.set(fields[1]);
			}
			context.write(newKey, newValue);
		}
	} 
	
	public static class ClientUserInstallFirstReduce extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		private Text newKey = new Text();
		private HashMap<String, String> provinceInstallDateMap = new HashMap<String, String>();
		private HashMap<String, String> ispInstallDateMap = new HashMap<String, String>();
		private HashMap<String, String> versionInstallDateMap = new HashMap<String, String>();
		private HashMap<String, HashMap<String, Long>> provinceHsDateMap = new HashMap<String, HashMap<String, Long>>();
		private HashMap<String, HashMap<String, Long>> ispHsDateMap = new HashMap<String, HashMap<String, Long>>();
		private HashMap<String, HashMap<String, Long>> versionHsDateMap = new HashMap<String, HashMap<String, Long>>();
		private HashMap<String, Long> dateHsDateMap = new HashMap<String, Long>();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			provinceInstallDateMap.clear();
			ispInstallDateMap.clear();
			versionInstallDateMap.clear();
			provinceHsDateMap.clear();
			ispHsDateMap.clear();
			versionHsDateMap.clear();
			dateHsDateMap.clear();
			String provinceId = null;
			String ispId = null;
			String versionId = null;
			String logDate = null;
			int installFlag = 0;
			StringBuilder hsDateSB = new StringBuilder();
			StringBuilder installDateSB = new StringBuilder();
			String date  = null;
			long downloadFlux = 0;
			
			for(Text val : values){
				String[] fields = val.toString().split(",");
				if(fields.length != 4){
					for(int i = 1; i < fields.length; i += 5){         //hs_log
						provinceId = fields[i];
						ispId = fields[i+1];
						versionId = fields[i+2];
						String[] dateDownloadStr = fields[i+4].split(":");
						
						for(int j = 0; j < dateDownloadStr.length; j += 2){
							date = dateDownloadStr[j];
							downloadFlux = Long.parseLong(dateDownloadStr[j+1]);
							if(dateHsDateMap.containsKey(date)){                               //日期
								dateHsDateMap.put(date, dateHsDateMap.get(date) + downloadFlux);
							}else{
								dateHsDateMap.put(date, downloadFlux);
							}
							if(provinceHsDateMap.containsKey(provinceId)){                                //省份
								HashMap<String, Long> hashmap = provinceHsDateMap.get(provinceId);
								if(hashmap.containsKey(date)){
									hashmap.put(date, hashmap.get(date) + downloadFlux);
								}else{
									hashmap.put(date, downloadFlux);
								}
								provinceHsDateMap.put(provinceId, hashmap);
							}else{
								HashMap<String, Long> hashmap = new HashMap<String, Long>();
								hashmap.put(date, downloadFlux);
								provinceHsDateMap.put(provinceId, hashmap);
							}
							if(ispHsDateMap.containsKey(ispId)){                                //isp
								HashMap<String, Long> hashmap = ispHsDateMap.get(ispId);
								if(hashmap.containsKey(date)){
									hashmap.put(date, hashmap.get(date) + downloadFlux);
								}else{
									hashmap.put(date, downloadFlux);
								}
								ispHsDateMap.put(ispId, hashmap);
							}else{
								HashMap<String, Long> hashmap = new HashMap<String, Long>();
								hashmap.put(date, downloadFlux);
								ispHsDateMap.put(ispId, hashmap);
							}
							if(versionHsDateMap.containsKey(versionId)){                                //isp
								HashMap<String, Long> hashmap = versionHsDateMap.get(versionId);
								if(hashmap.containsKey(date)){
									hashmap.put(date, hashmap.get(date) + downloadFlux);
								}else{
									hashmap.put(date, downloadFlux);
								}
								versionHsDateMap.put(versionId, hashmap);
							}else{
								HashMap<String, Long> hashmap = new HashMap<String, Long>();
								hashmap.put(date, downloadFlux);
								versionHsDateMap.put(versionId, hashmap);
							}
						}
					}
				}else{                     //wt_bh
					installFlag = 1;
					provinceId = fields[0];
					ispId = fields[1];
					versionId = fields[2];
					logDate = fields[3];
					if(installDateSB.length() > 0){
						installDateSB.append(":");
					}
					installDateSB.append(logDate);
					if(provinceInstallDateMap.containsKey(provinceId)){
						provinceInstallDateMap.put(provinceId, provinceInstallDateMap.get(provinceId) + ":" + logDate);
					}else{
						provinceInstallDateMap.put(provinceId, logDate);
					}
					if(ispInstallDateMap.containsKey(ispId)){
						ispInstallDateMap.put(ispId, ispInstallDateMap.get(ispId) + ":" + logDate);
					}else{
						ispInstallDateMap.put(ispId, logDate);
					}
					if(versionInstallDateMap.containsKey(versionId)){
						versionInstallDateMap.put(versionId, versionInstallDateMap.get(versionId) + ":" + logDate);
					}else{
						versionInstallDateMap.put(versionId, logDate);
					}
				}
			}
			
			if(installFlag == 1){
				newKey.set("d");
				for(String mapKey : dateHsDateMap.keySet()){
					if(hsDateSB.length() > 0){
						hsDateSB.append(":");
					}
					if(dateHsDateMap.get(mapKey) >= 20){
						hsDateSB.append(mapKey);
					}
				}
				if(hsDateSB.length() > 0){
					newValue.set(installDateSB.append(",").append(hsDateSB).toString());
				}else{
					newValue.set(installDateSB.append(",").append("-").toString());
				}
				context.write(newKey, newValue);
				for(String mapKey : provinceInstallDateMap.keySet()){
					newKey.set("p" + mapKey);
					if(provinceHsDateMap.containsKey(mapKey)){
						HashMap<String, Long> hashmap = provinceHsDateMap.get(mapKey);
						String str = "";
						for(String d : hashmap.keySet()){
							if(str.length() > 0){
								str += ":";
							}
							if(hashmap.get(d) >= 20){
								str += d;
							}
						}
						str = str.length() > 0 ? str : "-";
						newValue.set(provinceInstallDateMap.get(mapKey) + "," + str);
					}else{
						newValue.set(provinceInstallDateMap.get(mapKey) + "," + "-");
					}
					context.write(newKey, newValue);
				}
				for(String mapKey : ispInstallDateMap.keySet()){
					newKey.set("i" + mapKey);
					if(ispHsDateMap.containsKey(mapKey)){
						HashMap<String, Long> hashmap = ispHsDateMap.get(mapKey);
						String str = "";
						for(String d : hashmap.keySet()){
							if(str.length() > 0){
								str += ":";
							}
							if(hashmap.get(d) >= 20){
								str += d;
							}
						}
						str = str.length() > 0 ? str : "-";
						newValue.set(ispInstallDateMap.get(mapKey) + "," + str);
					}else{
						newValue.set(ispInstallDateMap.get(mapKey) + "," + "-");
					}
					context.write(newKey, newValue);
				}
				for(String mapKey : versionInstallDateMap.keySet()){
					newKey.set("v" + mapKey);
					if(versionHsDateMap.containsKey(mapKey)){
						HashMap<String, Long> hashmap = versionHsDateMap.get(mapKey);
						String str = "";
						for(String d : hashmap.keySet()){
							if(str.length() > 0){
								str += ":";
							}
							if(hashmap.get(d) >= 20){
								str += d;
							}
						}
						str = str.length() > 0 ? str : "-";
						newValue.set(versionInstallDateMap.get(mapKey) + "," + str);
					}else{
						newValue.set(versionInstallDateMap.get(mapKey) + "," + "-");
					}
					context.write(newKey, newValue);
				}
			}
		}
	}

	public static class ClientUserInstallSecondMapper extends
			Mapper<Text, Text, Text, Text>{
		
		@Override
		public void map(Text key, Text value, Context context) 
				throws IOException, InterruptedException{
			context.write(key, value);
		}
	} 
	
	public static class ClientUserInstallSecondReduce extends  
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
				throws IOException, InterruptedException {
			long installNum = 0;
			long effectInstallNum = 0;
			
			for(Text val : values){
				Set<String> set = new HashSet<String>();
				HashMap<String, Integer> dateNumMap = new HashMap<String, Integer>();
				String[] fields = val.toString().split(",");
				String[] installDates = fields[0].split(":");
				for(int i = 0; i < installDates.length; i++){
					String installDate = installDates[i];
					if(dateNumMap.containsKey(installDate)){
						dateNumMap.put(installDate, dateNumMap.get(installDate) + 1);
					}else{
						dateNumMap.put(installDate, 1);
					}
				}
				String[] hsDates = fields[1].split(":");
				for(int i = 0; i < hsDates.length; i++){
					if(!hsDates[i].equals("-")){
						set.add(hsDates[i]);						
					}
				}
				
				for(String mapKey : dateNumMap.keySet()){
					installNum += dateNumMap.get(mapKey);
					if(set.contains(mapKey)){
						effectInstallNum += dateNumMap.get(mapKey);
					}
				}
			}
			
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

			multipleOutputs.write(newKey, new Text("13" + "\t" + installNum), outputDir);
			multipleOutputs.write(newKey, new Text("14" + "\t" + effectInstallNum), outputDir);
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

		Job job = new Job(conf, "ClientUserInstallMR");
		job.setJarByClass(ClientUserInstallMR.class);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		String outputDir = conf.get("output_dir");
		
		String tmpDir = outputDir + "_tmp";
		Path tmpOutput = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, tmpOutput);
		tmpOutput.getFileSystem(conf).delete(tmpOutput, true);

		job.setMapperClass(ClientUserInstallFirstMapper.class);
		job.setReducerClass(ClientUserInstallFirstReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(30);

		int code = job.waitForCompletion(true) ? 0 : 1;

		if(code == 0){
			Job secondJob = new Job(conf, "ClientUserInstallResult");
			secondJob.setJarByClass(ClientUserInstallMR.class);
			conf.set("stat_date", conf.get("stat_date"));

			FileInputFormat.addInputPath(secondJob, new Path(tmpDir));
			Path output = new Path(outputDir);
			FileOutputFormat.setOutputPath(secondJob, output);
			output.getFileSystem(conf).delete(output, true);
	
			secondJob.setMapperClass(ClientUserInstallSecondMapper.class);
			secondJob.setReducerClass(ClientUserInstallSecondReduce.class);
	
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
		int res = ToolRunner.run(new Configuration(), new ClientUserInstallMR(), args);
		System.out.println(res);
	}
}
