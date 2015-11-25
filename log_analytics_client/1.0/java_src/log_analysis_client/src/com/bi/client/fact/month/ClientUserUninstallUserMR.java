package com.bi.client.fact.month;

import java.io.IOException;
import java.util.HashSet;
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

import com.bi.client.uninstall.format.UninstallLogFormatEnum;

public class ClientUserUninstallUserMR extends Configured implements Tool{
	
	public static class UninstallUserFirstMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		
		private static final int MACCOLNUM = UninstallLogFormatEnum.MAC.ordinal();
		private static final int PROVINCECOLNUM = UninstallLogFormatEnum.PROVINCE_ID.ordinal();   
		private static final int VERSIONCOLNUM = UninstallLogFormatEnum.VERSION_ID.ordinal();
		private static final int ISPCOLNUM = UninstallLogFormatEnum.ISP_ID.ordinal();
		private Text newKey = new Text();
		private Text newValue = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			
			String[] fields = value.toString().trim().split("\t");
			String provinceId = fields[PROVINCECOLNUM];
			String versionId = fields[VERSIONCOLNUM];
			String ispId = fields[ISPCOLNUM];
			String mac = fields[MACCOLNUM];
			
			if(!mac.equals("-")){
				newKey.set(mac);
				newValue.set(provinceId + "\t" + ispId + "\t" + versionId);
				context.write(newKey, newValue);
			}
		}
	}
	
	public static class UninstallUserFirstReduce extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		private Set<String> provinceSet = new HashSet<String>();
		private Set<String> ispSet = new HashSet<String>();
		private Set<String> versionSet = new HashSet<String>();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			provinceSet.clear();
			ispSet.clear();
			versionSet.clear();
			StringBuilder provinceSB = new StringBuilder();
			StringBuilder ispSB = new StringBuilder();
			StringBuilder versionSB = new StringBuilder();
			String provinceId = null;
			String ispId = null;
			String versionId = null;
			
			for(Text val : values){
				String[] fields = val.toString().split("\t");
				provinceId = fields[0];
				ispId = fields[1];
				versionId = fields[2];
				if(!provinceSet.contains(provinceId)){
					provinceSet.add(provinceId);
					if(provinceSB.length() > 0){
						provinceSB.append(",");
					}
					provinceSB.append(provinceId);
				}
				if(!ispSet.contains(ispId)){
					ispSet.add(ispId);
					if(ispSB.length() > 0){
						ispSB.append(",");
					}
					ispSB.append(ispId);
				}
				if(!versionSet.contains(versionId)){
					versionSet.add(versionId);
					if(versionSB.length() > 0){
						versionSB.append(",");
					}
					versionSB.append(versionId);
				}
			}
			newValue.set(provinceSB.toString() + "\t" + ispSB.toString() + "\t" + versionSB.toString());
			context.write(key, newValue);
		}		
	}
	
	public static class UninstallUserSecondMapper extends
			Mapper<LongWritable, Text, Text, Text>{

		private Text newValue = new Text();
		private Text newKey = new Text();
		private String date = null;
		private static Logger logger = Logger.getLogger(ClientUserUninstallUserMR.class.getName());
		
		@Override
		public void setup(Context context){
			date = context.getConfiguration().get("stat_date");
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			try {
				String[] fields = value.toString().split("\t");
				String[] provinceStrs = fields[1].split(",");
				String[] ispStrs = fields[2].split(",");
				String[] versionStrs = fields[3].split(",");
				
				for(int i = 0; i < provinceStrs.length; i++){
					newKey.set("p" + provinceStrs[i]);
					newValue.set("1");
					context.write(newKey, newValue);
				}
				
				for(int i = 0; i < ispStrs.length; i++){
					newKey.set("i" + ispStrs[i]);
					newValue.set("1");
					context.write(newKey, newValue);
				}
				
				for(int i = 0; i < versionStrs.length; i ++){
					newKey.set("v" + versionStrs[i]);
					newValue.set("1");
					context.write(newKey, newValue);
				}
				
				newKey.set("d" + date);
				newValue.set("1");
				context.write(newKey, newValue);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("error originalData: " + key.toString() + "\t" + value.toString());
			}
		}
	}
	
	public static class UninstallUserSecondCombiner extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			int count = 0;
			for(Text val : values){
				count += Integer.parseInt(val.toString());
			}
			newValue.set(Integer.toString(count));
			context.write(key, newValue);
		}
	}
	
	public static class UninstallUserSecondReduce extends
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
			int uninstallUserNum = 0;
			for(Text val : values){
				uninstallUserNum += Integer.parseInt(val.toString());
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
				newKey.set(key.toString().substring(1));
			}else{
				newKey.set(date + "\t" + key.toString().substring(1));
			}

			multipleOutputs.write(newKey, new Text("15" + "\t" + uninstallUserNum), outputDir);
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
		
		Job job = new Job(conf, "UninstallUserMR");
		job.setJarByClass(ClientUserUninstallUserMR.class);
		conf.set("stat_date", conf.get("stat_date"));
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		String outputDir = conf.get("output_dir");
		String tmpDir = outputDir + "_tmp";
		Path tmpOutput = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, tmpOutput);
		tmpOutput.getFileSystem(conf).delete(tmpOutput, true);
		
		job.setMapperClass(UninstallUserFirstMapper.class);
		job.setReducerClass(UninstallUserFirstReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(10);
		
		int code = job.waitForCompletion(true) ? 0 : 1;
		
		if(code == 0){
			Job secondJob = new Job(conf, "UninstallUserResult");
			secondJob.setJarByClass(ClientUserUninstallUserMR.class);
			
			FileInputFormat.addInputPath(secondJob, new Path(tmpDir));
			Path output = new Path(outputDir);
			FileOutputFormat.setOutputPath(secondJob, output);
			output.getFileSystem(conf).delete(output, true);
			
			secondJob.setMapperClass(UninstallUserSecondMapper.class);
			secondJob.setCombinerClass(UninstallUserSecondCombiner.class);
			secondJob.setReducerClass(UninstallUserSecondReduce.class);
			
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
		int res = ToolRunner.run(new Configuration(), new ClientUserUninstallUserMR(), args);
		System.out.println(res);
	}
}
