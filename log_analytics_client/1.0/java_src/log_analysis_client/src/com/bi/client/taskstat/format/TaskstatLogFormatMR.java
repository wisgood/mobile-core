package com.bi.client.taskstat.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.bi.client.util.IpParser1;

public class TaskstatLogFormatMR extends Configured implements Tool {
	
	public static class TaskstatLogFormatMap extends
			Mapper<LongWritable, Text, Text, Text>{
		
		private static String date = null;
		private static Logger logger = Logger.getLogger(TaskstatLogFormatMR.class.getName());
		private IpParser1 ipParser = null;
		
		public void setup(Context context) throws IOException, InterruptedException{
			ipParser = new IpParser1();
			try{
				ipParser.init("ip_table");
				Configuration conf = context.getConfiguration();
				date = conf.get("stat_date");
			}catch(IOException e){
				e.printStackTrace();
				System.exit(0);
			}	
		}
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String taskstatFormatStr = null;
			try {
				String[] fields = value.toString().split("\t");
				if(fields.length == TaskstatLogEnum.ISP_ID.ordinal()+1){
					TaskstatLog taskstatLog = new TaskstatLog();
					taskstatLog.setIpParser(ipParser);
					taskstatLog.setFields(fields);
					taskstatFormatStr = date + "\t" + taskstatLog.toString();
					
					if(taskstatFormatStr != null && !("".equals(taskstatFormatStr))){
						context.write(new Text(taskstatLog.getTimestamp()), new Text(taskstatFormatStr));
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("error originalData: " + taskstatFormatStr);
				logger.error(e.getMessage(), e.getCause());
			}

		}
	}
	
	public static class TaskstatLogFormatReduce extends
			Reducer<Text, Text, NullWritable, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) 
						throws IOException, InterruptedException{
			for(Text value : values){
				context.write(NullWritable.get(), value);
			}
		}
	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();
		
		Job job = new Job(conf, "TaskstatLogFormat");
		String statDate = conf.get("stat_date");
		conf.set("stat_date", statDate);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));                   //输入、输出文件路径
		Path output = new Path(conf.get("output_dir"));
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
        
		job.setJarByClass(TaskstatLogFormatMR.class);
		job.setMapperClass(TaskstatLogFormatMap.class);	
		job.setReducerClass(TaskstatLogFormatReduce.class);
		job.setInputFormatClass(TextInputFormat.class);               																			//输入、输出类型
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int nRet = ToolRunner.run(new Configuration(), new TaskstatLogFormatMR(), args);
		System.out.println(nRet);
	}
}
