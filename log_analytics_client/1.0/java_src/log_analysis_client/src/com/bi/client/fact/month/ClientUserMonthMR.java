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

public class ClientUserMonthMR extends Configured implements Tool {
	
	public static class ClientUserMonthFirstMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		
		private String pathName = null;
		private Text newValue = new Text();
		private Text newKey = new Text();
		
		@Override
		public void setup(Context context){
			pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			if(Pattern.matches(".*/1_history_mac/.*", pathName)){
				String mac = value.toString();
				newKey.set(mac);
				newValue.set("historyMac" + "," + "1");
			}else{
				String[] fields = value.toString().split("\t");
				String logDate = fields[0];
				String versionId = fields[1];
				String provinceId = fields[2];
				String mac = fields[3];
				String ispId = fields[4];
				String dim = "p" + provinceId + "," + "i" + ispId + "," + "v" + versionId;
				newKey.set(mac);
			
				if(Pattern.matches(".*/hs_log/.*", pathName)){
					int downloadFlux = Integer.parseInt(fields[19]);
					int onlineTime = Integer.parseInt(fields[14]) - Integer.parseInt(fields[13]);
				
					newValue.set("hs_log" + "," + dim + "," + logDate + "," + onlineTime + "," + downloadFlux);
					context.write(newKey, newValue);
				}else if(Pattern.matches(".*/install/.*", pathName)){
					newValue.set("install" + "," + dim + "," + logDate);
					context.write(newKey, newValue);
				}else if(Pattern.matches(".*/wtbh/.*", pathName)){
					String playTime = fields[5];
					newValue.set("wtbh" + "," + dim + "," + playTime);
					context.write(newKey, newValue);
				}
			}
		}
	}
	
	public static class ClientUserMonthFirstReduce extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		private Text newKey = new Text();
		private String date = null;
		private HashMap<String, Integer> provinceOnlineTimeMap = new HashMap<String, Integer>();
		private HashMap<String, HashMap<String, Integer>> provinceDateDownloadMap = new HashMap<String, HashMap<String, Integer>>();
		private HashMap<String, String> provinceInstallDateMap = new HashMap<String, String>();
		private HashMap<String, Integer> provincePlayTimeMap = new HashMap<String, Integer>();

		private HashMap<String, Integer> ispOnlineTimeMap = new HashMap<String, Integer>();
		private HashMap<String, HashMap<String, Integer>> ispDateDownloadMap = new HashMap<String, HashMap<String, Integer>>();
		private HashMap<String, String> ispInstallDateMap = new HashMap<String, String>();
		private HashMap<String, Integer> ispPlayTimeMap = new HashMap<String, Integer>();
		
		private HashMap<String, Integer> versionOnlineTimeMap = new HashMap<String, Integer>();
		private HashMap<String, HashMap<String, Integer>> versionDateDownloadMap = new HashMap<String, HashMap<String, Integer>>();
		private HashMap<String, String> versionInstallDateMap = new HashMap<String, String>();
		private HashMap<String, Integer> versionPlayTimeMap = new HashMap<String, Integer>();
		
		private Set<String> provinceSet = new HashSet<String>();
		private Set<String> ispSet = new HashSet<String>();
		private Set<String> versionSet = new HashSet<String>();
		
		long dateOnlineTime = 0;
		long datePlayTime = 0;
		private StringBuilder dateInstallSB = new StringBuilder();
		private HashMap<String, Integer> dateDownloadMap = new HashMap<String, Integer>();
		
//		private static Logger logger = Logger.getLogger(ClientUserMonthMR.class.getName());
		
		public void setup(Context context){
			date = context.getConfiguration().get("stat_date");
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			dateOnlineTime = 0;
			datePlayTime = 0;
			dateDownloadMap.clear();
			provinceOnlineTimeMap.clear();
			provinceDateDownloadMap.clear();
			provinceInstallDateMap.clear();
			provincePlayTimeMap.clear();
			ispOnlineTimeMap.clear();
			ispDateDownloadMap.clear();
			ispInstallDateMap.clear();
			ispPlayTimeMap.clear();
			versionOnlineTimeMap.clear();
			versionDateDownloadMap.clear();
			versionInstallDateMap.clear();
			versionPlayTimeMap.clear();
			provinceSet.clear();
			ispSet.clear();
			versionSet.clear();
			String province = null;
			String isp = null;
			String version = null;
			String logType = null;
			String logDate = null;
			int historyMac = 0;
			int onlineTime = 0;
			int playTime = 0;
			int downloadFlux = 0;

			for(Text val : values){
				String[] fields = val.toString().split(",");
				if(fields.length == 1){      //history_mac flag
					historyMac = 1;
				}else{
					logType = fields[0];
					province = fields[1];
					isp = fields[2];
					version = fields[3];
					
					provinceSet.add(province);
					ispSet.add(isp);
					versionSet.add(version);
					
					if(logType.equals("hs_log")){   //hs_log
						logDate = fields[4];
						onlineTime = Integer.parseInt(fields[5]);
						downloadFlux = Integer.parseInt(fields[6]);
						
						dateOnlineTime += onlineTime;
						if(dateDownloadMap.containsKey(logDate)){
							dateDownloadMap.put(logDate, dateDownloadMap.get(logDate) + downloadFlux);
						}else{
							dateDownloadMap.put(logDate, downloadFlux);
						}
						
						if(provinceOnlineTimeMap.containsKey(province)){
							provinceOnlineTimeMap.put(province, provinceOnlineTimeMap.get(province) + onlineTime);
							HashMap<String, Integer> hashmap = provinceDateDownloadMap.get(province);
							if(hashmap.containsKey(logDate)){
								hashmap.put(logDate, hashmap.get(logDate) + downloadFlux);
							}else{
								hashmap.put(logDate, downloadFlux);
							}
							provinceDateDownloadMap.put(province, hashmap);
						}else{
							provinceOnlineTimeMap.put(province, onlineTime);
							HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
							hashmap.put(logDate, downloadFlux);
							provinceDateDownloadMap.put(province, hashmap);
						}
						
						if(ispOnlineTimeMap.containsKey(isp)){
							ispOnlineTimeMap.put(isp, ispOnlineTimeMap.get(isp) + onlineTime);
							HashMap<String, Integer> hashmap = ispDateDownloadMap.get(isp);
							if(hashmap.containsKey(logDate)){
								hashmap.put(logDate, hashmap.get(logDate) + downloadFlux);
							}else{
								hashmap.put(logDate, downloadFlux);
							}
							ispDateDownloadMap.put(isp, hashmap);
						}else{
							ispOnlineTimeMap.put(isp, onlineTime);
							HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
							hashmap.put(logDate, downloadFlux);
							ispDateDownloadMap.put(isp, hashmap);
						}
						
						if(versionOnlineTimeMap.containsKey(version)){
							versionOnlineTimeMap.put(version, provinceOnlineTimeMap.get(version) + onlineTime);
							HashMap<String, Integer> hashmap = versionDateDownloadMap.get(version);
							if(hashmap.containsKey(logDate)){
								hashmap.put(logDate, hashmap.get(logDate) + downloadFlux);
							}else{
								hashmap.put(logDate, downloadFlux);
							}
							versionDateDownloadMap.put(version, hashmap);
						}else{
							versionOnlineTimeMap.put(version, onlineTime);
							HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
							hashmap.put(logDate, downloadFlux);
							versionDateDownloadMap.put(version, hashmap);
						}
					}else if(logType.equals("install")){
						logDate = fields[4];
						
						if(dateInstallSB.length() > 0){
							dateInstallSB.append(":");
						}
						dateInstallSB.append(logDate);
						
						if(provinceInstallDateMap.containsKey(province)){
							provinceInstallDateMap.put(province, provinceInstallDateMap.get(province) + ":" + logDate);
						}else{
							provinceInstallDateMap.put(province, logDate);
						}
						
						if(ispInstallDateMap.containsKey(isp)){
							ispInstallDateMap.put(isp, ispInstallDateMap.get(isp) + ":" + logDate);
						}else{
							ispInstallDateMap.put(isp, logDate);
						}
						
						if(versionInstallDateMap.containsKey(version)){
							versionInstallDateMap.put(version, versionInstallDateMap.get(version) + ":" + logDate);
						}else{
							versionInstallDateMap.put(version, logDate);
						}
					}else if(logType.equals("wtbh")){
						playTime = Integer.parseInt(fields[4]);
						
						datePlayTime += playTime;
						
						if(provincePlayTimeMap.containsKey(province)){
							provincePlayTimeMap.put(province, provincePlayTimeMap.get(province) + playTime);
						}else{
							provincePlayTimeMap.put(province, playTime);
						}
						
						if(ispPlayTimeMap.containsKey(isp)){
							ispPlayTimeMap.put(isp, ispPlayTimeMap.get(isp) + playTime);
						}else{
							ispPlayTimeMap.put(isp, playTime);
						}
						
						if(versionPlayTimeMap.containsKey(version)){
							versionPlayTimeMap.put(version, versionPlayTimeMap.get(version) + playTime);
						}else{
							versionPlayTimeMap.put(version, playTime);
						}
					}
				}
			}
			 
			 //dim : province
			for(Iterator<String> iterator = provinceSet.iterator(); iterator.hasNext(); ){
				StringBuilder provinceSB = new StringBuilder();
				String mapKey = iterator.next();
				if(provinceOnlineTimeMap.containsKey(mapKey)){
					provinceSB.append(provinceOnlineTimeMap.get(mapKey));
					HashMap<String, Integer> hashmap = provinceDateDownloadMap.get(mapKey);
					for(String dateKey : hashmap.keySet()){
						int download = hashmap.get(dateKey);
						if(download > 20){
							provinceSB.append(":").append(dateKey);//.append(":").append(hashmap.get(dateKey));
						}
					}
				}else{
					provinceSB.append("-");
				}
				
				if(provinceInstallDateMap.containsKey(mapKey)){
					provinceSB.append(",").append(provinceInstallDateMap.get(mapKey) );
				}else{
					provinceSB.append(",").append("-");
				}
				
				if(provincePlayTimeMap.containsKey(mapKey)){
					provinceSB.append(",").append(provincePlayTimeMap.get(mapKey));
				}else{
					provinceSB.append(",").append("-");
				}
				
				provinceSB.append(",").append(historyMac);
				newKey.set(mapKey);
				newValue.set(provinceSB.toString());
				context.write(newKey, newValue);
			}
			
			 //dim : isp
			for(Iterator<String> iterator = ispSet.iterator(); iterator.hasNext(); ){
				StringBuilder ispSB = new StringBuilder();
				String mapKey = iterator.next();
				if(ispOnlineTimeMap.containsKey(mapKey)){
					ispSB.append(ispOnlineTimeMap.get(mapKey));
					HashMap<String, Integer> hashmap = ispDateDownloadMap.get(mapKey);
					for(String dateKey : hashmap.keySet()){
						int download = hashmap.get(dateKey);
						if(download > 20){
							ispSB.append(":").append(dateKey);//.append(":").append(hashmap.get(dateKey));
						}
					}
				}else{
					ispSB.append("-");
				}
				
				if(ispInstallDateMap.containsKey(mapKey)){
					ispSB.append(",").append(ispInstallDateMap.get(mapKey) );
				}else{
					ispSB.append(",").append("-");
				}
				
				if(ispPlayTimeMap.containsKey(mapKey)){
					ispSB.append(",").append(ispPlayTimeMap.get(mapKey));
				}else{
					ispSB.append(",").append("-");
				}
				
				ispSB.append(",").append(historyMac);
				newKey.set(mapKey);
				newValue.set(ispSB.toString());
				context.write(newKey, newValue);
			}
			
			//dim : version
			for(Iterator<String> iterator = versionSet.iterator(); iterator.hasNext(); ){
				StringBuilder versionSB = new StringBuilder();
				String mapKey = iterator.next();
				if(versionOnlineTimeMap.containsKey(mapKey)){
					versionSB.append(versionOnlineTimeMap.get(mapKey));
					HashMap<String, Integer> hashmap = versionDateDownloadMap.get(mapKey);
					for(String dateKey : hashmap.keySet()){
						int download = hashmap.get(dateKey);
						if(download > 20){
							versionSB.append(":").append(dateKey);//.append(":").append(hashmap.get(dateKey));
						}
					}
				}else{
					versionSB.append("-");
				}
				
				if(versionInstallDateMap.containsKey(mapKey)){
					versionSB.append(",").append(versionInstallDateMap.get(mapKey) );
				}else{
					versionSB.append(",").append("-");
				}
				
				if(versionPlayTimeMap.containsKey(mapKey)){
					versionSB.append(",").append(versionPlayTimeMap.get(mapKey));
				}else{
					versionSB.append(",").append("-");
				}
				
				versionSB.append(",").append(historyMac);
				newKey.set(mapKey);
				newValue.set(versionSB.toString());
				context.write(newKey, newValue);
			}
			
			StringBuilder dateSB = new StringBuilder();                                                    //dim : date
			dateSB.append(dateOnlineTime);
			for(String mapKey : dateDownloadMap.keySet()){
				int download = dateDownloadMap.get(mapKey);
				if(download > 20){
					dateSB.append(":").append(mapKey);//.append(":").append(dateDownloadMap.get(mapKey));
				}
			}
			dateSB.append(",").append(dateInstallSB).append(",").append(datePlayTime).append(",").append(historyMac);
			newKey.set("d" + date);
			newValue.set(dateSB.toString());
			context.write(newKey, newValue);
		}
	}
		
	public static class ClientUserMonthSecondMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newValue = new Text();
		private Text newKey = new Text();
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] fields = value.toString().split("\t");
			newKey.set(fields[0]);
			newValue.set(value.toString().substring((fields[0]+"\t").length()));
			context.write(newKey, newValue);
		}
	}
	
	public static class ClientUserMonthSecondReduce extends
			Reducer<Text, Text, Text, Text>{
		private Text newKey = new Text();
		private String date = null;
		private long onlineTime = 0;
		private long playTime = 0;
		private long onlineUser = 0;
		private long playUser = 0;
		private long installNum = 0;
		private long effectInstallNum =0;
		private long newUser = 0;
		private MultipleOutputs<Text, Text> multipleOutputs = null;
		
		public void setup(Context context){
			date = context.getConfiguration().get("stat_date");
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			for(Text val : values){
				Set<String> dateDownloadSet = new HashSet<String>();
				HashMap<String, Integer> dateInstallMap = new HashMap<String, Integer>();
				String[] fields = val.toString().split(",");
				String hsLogStr = fields[0];
				String installLogStr = fields[1];
				String wtbhLogStr = fields[2];
				String historyMac = fields[3];
				if(!hsLogStr.equals("-")){
					if(historyMac.equals("1")){
						newUser += 1;
					}
					onlineUser += 1;
					String[] strs = hsLogStr.split(":");
					onlineTime += Integer.parseInt(strs[0]);
					for(int j = 1; j < strs.length; j++){
						dateDownloadSet.add(strs[j]);
					}
				}
				
				if(!installLogStr.equals("-")){
					String[] strs = installLogStr.split(",");
					for(int j = 0; j < strs.length; j++){
						if(dateInstallMap.containsKey(strs[j])){
							dateInstallMap.put(strs[j], dateInstallMap.get(strs[j]) + 1);
						}else{
							dateInstallMap.put(strs[j], 1);
						}
					}
				}
				
				if(!wtbhLogStr.equals("-")){
					playUser += 1;
					playTime += Integer.parseInt(wtbhLogStr);
				}
				
				for(String installDate : dateInstallMap.keySet()){
					int num = dateInstallMap.get(installDate);
					installNum += num;
					if(dateDownloadSet.contains(installDate)){
						effectInstallNum += num;
					}
				}
			}
			
			double averageOnlineTime = (double)onlineTime/onlineUser;
			double averageUserPlayTime = (double)playTime/onlineUser;
			double averagePlayTime = (double)playTime/playUser;
			double hourOnlineTime = (double)onlineTime/36000000;
			double hourPlayTime = (double)playTime/36000000;
			
			String flag = key.toString().substring(0, 1);
			String outputDir = "";
			if(flag.equals("p")){
				outputDir = "F_MONTH_CLIENT_DATE_AREA"; 
			}else if(flag.equals("i")){
				outputDir = "F_MONTH_CLIENT_DATE_ISP";
			}else if(flag.equals("v")){
				outputDir = "F_MONTH_CLIENT_DATE_VERSION";
			}else{
				outputDir = "F_MONTH_CLIENT_DATE_AREA";
			}
			
			if(flag.equals("d")){
				newKey.set(key.toString().substring(1));
			}else{
				newKey.set(date + "\t" + key.toString().substring(1));
			}

			multipleOutputs.write(newKey, new Text("1" + "\t" + onlineUser), outputDir);
			multipleOutputs.write(newKey, new Text("5" + "\t" + hourOnlineTime), outputDir);
			multipleOutputs.write(newKey, new Text("6" + "\t" + averageOnlineTime), outputDir);
			multipleOutputs.write(newKey, new Text("7" + "\t" + newUser), outputDir);
			multipleOutputs.write(newKey, new Text("13" + "\t" + installNum), outputDir);
			multipleOutputs.write(newKey, new Text("14" + "\t" + effectInstallNum), outputDir);
			multipleOutputs.write(newKey, new Text("17" + "\t" + hourPlayTime), outputDir);
			multipleOutputs.write(newKey, new Text("20" + "\t" + playUser), outputDir);
			multipleOutputs.write(newKey, new Text("21" + "\t" + averageUserPlayTime), outputDir);
			multipleOutputs.write(newKey, new Text("22" + "\t" + averagePlayTime), outputDir);

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

		Job job = new Job(conf, "ClientUserMonthMR");
		job.setJarByClass(ClientUserMonthMR.class);
		conf.set("stat_date", conf.get("stat_date"));
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		String outputDir = conf.get("output_dir");
		String tmpDir = outputDir + "_tmp";
		Path tmpOutput = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, tmpOutput);
		tmpOutput.getFileSystem(conf).delete(tmpOutput, true);

		job.setMapperClass(ClientUserMonthFirstMapper.class);
		job.setReducerClass(ClientUserMonthFirstReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(conf.getInt("reduce_num", 20));

		int code = job.waitForCompletion(true) ? 0 : 1;

		if(code == 0){
			Job secondJob = new Job(conf, "ClientUserMonthResult");
			secondJob.setJarByClass(ClientUserMonthMR.class);
			conf.set("stat_date", conf.get("stat_date"));
	
			FileInputFormat.addInputPath(secondJob, new Path(tmpDir));
			Path output = new Path(outputDir);
			FileOutputFormat.setOutputPath(secondJob, output);
			output.getFileSystem(conf).delete(output, true);
	
			secondJob.setMapperClass(ClientUserMonthSecondMapper.class);
			secondJob.setReducerClass(ClientUserMonthSecondReduce.class);
	
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
		int res = ToolRunner.run(new Configuration(), new ClientUserMonthMR(), args);
		System.out.println(res);
	}
}
