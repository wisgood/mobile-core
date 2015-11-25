package com.bi.client.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import com.bi.client.fact.month.HsLogFormatEnum;

public class UpdateHistoryMacQudaoMR extends Configured implements Tool {
	/**   
	 * All rights reserved
	 * www.funshion.com
	 *
	 * @Title: updateHistoryMacQudaoMR.java 
	 * @Package com.bi.client.common 
	 * @Description: update the history mac_qudao file with hs_log
	 * @author limm
	 * @date 2014-01-14 
	 * @input: /dw/logs/tools/origin/FsPlatformBoot/3/ 
	 * @output: /dw/logs/format/fsplatformboot
	 * @script:     
	 */
	public static class UpdateHistoryMacQudaoMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] fields = value.toString().split("\t");
			if(fields.length == 3){
				newKey.set(fields[0]);
				newValue.set("history" + "\t" + fields[1] + "\t" + fields[2]);
			}else{
				newKey.set(fields[HsLogFormatEnum.MAC.ordinal()]);
				newValue.set(fields[HsLogFormatEnum.CHANNELID.ordinal()] + "\t" + fields[HsLogFormatEnum.DATE_ID.ordinal()]);
			}
			context.write(newKey, newValue);
		}
	}
	
	public static class UpdateHistoryMacQudaoReducer extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		private MultipleOutputs<Text, Text> multipleOutputs = null;
		private String historyMacQudaoOutputDir = null;
		
		@Override
		public void setup(Context context){
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
			historyMacQudaoOutputDir  = "historyMacQudao/part";
		}
		
		public void reduce(Text key, Iterable <Text> values, Context context) 
				throws IOException, InterruptedException{
			String newQudaoId = null;
			String lastQudaoId = null;
			String qudaoId = null;
			String dateId = null;
			boolean isNewMac = true;
			for(Text val : values){
				String[] fields = val.toString().split("\t");
				if(fields.length == 3){
					isNewMac = false;
					qudaoId = fields[1];
					dateId = fields[2];
				}else{
					newQudaoId = fields[0];
					dateId = fields[1];
					 if (lastQudaoId == null) {
						 lastQudaoId = newQudaoId;
				     }
				     if (lastQudaoId.compareTo(newQudaoId) > 0) {
				         lastQudaoId = newQudaoId;
				     }
				}
			}
			if(isNewMac){
				qudaoId = lastQudaoId;
				newValue.set(qudaoId + "\t" + dateId);
				context.write(key, newValue);
			}
			newValue.set(qudaoId + "\t" + dateId);
			multipleOutputs.write(key, newValue, historyMacQudaoOutputDir);
		}
		
		@Override
		public void cleanup(Context context) throws IOException,InterruptedException {
			multipleOutputs.close();
		}
	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();
		
		Job job = new Job(conf, conf.get("job_name"));
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		Path output = new Path(conf.get("output_dir"));
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
		
		job.setJarByClass(UpdateHistoryMacQudaoMR.class);
		job.setMapperClass(UpdateHistoryMacQudaoMapper.class);
		job.setReducerClass(UpdateHistoryMacQudaoReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(20);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int nRet = ToolRunner.run(new Configuration(), new UpdateHistoryMacQudaoMR(), args);
		System.out.println(nRet);
	}
}
