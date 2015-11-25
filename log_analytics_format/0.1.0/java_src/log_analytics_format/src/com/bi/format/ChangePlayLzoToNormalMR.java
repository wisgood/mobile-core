package com.bi.format;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.CommonConstant;
import com.bi.common.util.HdfsUtil;


public class ChangePlayLzoToNormalMR extends Configured implements Tool {

		public static class ChangePlayLzoToNormalMaper extends
				Mapper<LongWritable, Text, Text, NullWritable> {
			//Text keyText = new Text();
			//Text valueText = new Text();
			
			public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException{
				context.write(value, NullWritable.get());	
			}
			
		}
		
		
		public static class ChangePlayLzoToNormalReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				context.write(key, NullWritable.get());
			}
	}
		
		public static void main(String[] args) throws Exception {
			// TODO Auto-generated method stub
			int nRet = ToolRunner
					.run(new Configuration(), new ChangePlayLzoToNormalMR(), args);
			System.out.println(nRet);
		}

		public int run(String[] args) throws Exception {

			Configuration conf = getConf();
			Job job = new Job(conf, "ChangePlayLzoToNormalMR");
			job.setJarByClass(ChangePlayLzoToNormalMR.class);
			job.setMapperClass(ChangePlayLzoToNormalMaper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setReducerClass(ChangePlayLzoToNormalReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			String inputPathStr = job.getConfiguration().get(CommonConstant.INPUT_PATH);
			String outputPathStr = job.getConfiguration().get(
					CommonConstant.OUTPUT_PATH);
			HdfsUtil.deleteDir(outputPathStr);
			FileInputFormat.setInputPaths(job, inputPathStr);
			FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
			int isInputLZOCompress = job.getConfiguration().getInt(
					CommonConstant.IS_INPUTFORMATLZOCOMPRESS, 1);
			if (1 == isInputLZOCompress) {
				job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
			}
			int result = job.waitForCompletion(true) ? 0 : 1;
			return result;
		}

}
