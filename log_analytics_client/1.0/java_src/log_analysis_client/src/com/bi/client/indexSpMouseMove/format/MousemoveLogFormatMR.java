package com.bi.client.indexSpMouseMove.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.client.util.ChannelIdInfo;
import com.bi.client.util.IpParser1;
import com.bi.client.util.StringSplit;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class MousemoveLogFormatMR extends Configured implements Tool {
	
	public static class MousemoveLogFormatMap extends
			Mapper<LongWritable, Text, NullWritable, Text>{
		
		private IpParser1 ipParser = null;
		private ChannelIdInfo channelIdInfo = null;
		
		@Override
		public void setup(Context context) throws IOException{
			ipParser = new IpParser1();
			channelIdInfo = new ChannelIdInfo();
			try {
				ipParser.init("ip_table");
				channelIdInfo.init("channelId_info");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(0);
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String mouseMoveFormatStr = null;
			String[] fields = StringSplit.splitLog(value.toString(), ',');
			if(fields.length == MousemoveLogEnum.FCK.ordinal()+1){
				MousemoveLog mouseMoveLog = new MousemoveLog();
				mouseMoveLog.setIpParser(ipParser);
				mouseMoveLog.setChannelIdMap(channelIdInfo.getChannelIdMap());
				mouseMoveLog.setFileds(fields);
				mouseMoveFormatStr = mouseMoveLog.toString();
				
				if(mouseMoveFormatStr != null && !("".equals(mouseMoveFormatStr))){
					context.write(NullWritable.get(), new Text(mouseMoveFormatStr));
				}
			}
		}
	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();
		
		Job job = new Job(conf, "MousemoveLogFormat");
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));                   //输入、输出文件路径
		Path output = new Path(conf.get("output_dir"));
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
        
		job.setJarByClass(MousemoveLogFormatMR.class);
		job.setMapperClass(MousemoveLogFormatMap.class);	
		job.setInputFormatClass(LzoTextInputFormat.class);               				//输入、输出类型
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int nRet = ToolRunner.run(new Configuration(), new MousemoveLogFormatMR(), args);
		System.out.println(nRet);
	}
	
}
