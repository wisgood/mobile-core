/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: Pv2LogFormatMR.java 
* @Package com.bi.client.pv2.format 
* @Description: 对日志名进行处理
* @author limm
* @date 2013-9-10 上午11:09:08 
* @input:输入日志路径/2013-9-10
* @output:输出日志路径/2013-9-10
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.client.pv2.format;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.client.util.ChannelIdInfo;
import com.bi.client.util.IpParser1;
import com.bi.client.util.StringSplit;
import com.hadoop.mapreduce.LzoTextInputFormat;

/** 
 * @ClassName: Pv2LogFormatMR 
 * @Description: 对pv2日志进行format
 * @inputPath: hdfs:/dw/logs/web/origin/pv/2
 * @outputPath: hdfs:/dw/logs/client/format/pv2
 * @author limm 
 * @date 2013-9-10 上午11:09:08  
 */
public class Pv2LogFormatMR extends Configured implements Tool {
	
	public static class Pv2LogFormatMap extends
			Mapper<LongWritable, Text, NullWritable, Text>{
		private IpParser1 ipParser = null;
		private ChannelIdInfo channelIdInfo = null;
		private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
		
		@Override
		public void setup(Context context) throws IOException{
			ipParser = new IpParser1();
			channelIdInfo = new ChannelIdInfo();
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			try {
				ipParser.init("ip_table");
				channelIdInfo.init("channelId_info");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(0);
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{

			String pvFormatStr = null;
			try {
				String[] fields = StringSplit.splitLog(value.toString(), '\t');
				if(fields.length == Pv2LogEnum.TA.ordinal()+1){
					Pv2Log pvLog = new Pv2Log();
					pvLog.setIpParser(ipParser);
					pvLog.setChannelIdMap(channelIdInfo.getChannelIdMap());
					pvLog.setFields(fields);
					pvFormatStr = pvLog.toString();
					if(pvFormatStr!=null && !("".equals(pvFormatStr))){
						context.write(NullWritable.get(), new Text(pvFormatStr));
					}
				}else{
					throw new Exception("length");
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				multipleOutputs.write(NullWritable.get(), new Text(e.getMessage() + "\t" + value), "_error/output");
			}
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
		
		Job job = new Job(conf, "Pv2LogFormat");
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));                   //输入、输出文件路径
		Path output = new Path(conf.get("output_dir"));
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
        
		job.setJarByClass(Pv2LogFormatMR.class);
		job.setMapperClass(Pv2LogFormatMap.class);	
		job.setInputFormatClass(LzoTextInputFormat.class);               			//输入、输出类型
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	/**
	 * @throws Exception  
	 * @Title: main 
	 * @Description:对pv2日志进行format 
	 * @param:
	 * @return void
	 * @Command:hadoop jar newSpecial.jar com.bi.client.pv2.format 
	 * 								-files files/ip_table,files/channelId_info 
	 * 								-D input_dir=input_path 
	 * 								-D output_dir=output_path  
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int nRet = ToolRunner.run(new Configuration(), new Pv2LogFormatMR(), args);
		System.out.println(nRet);
	}
}
