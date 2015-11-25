package com.bi.logs.format;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.util.BeanFactoryUtil;
import com.bi.common.util.HdfsUtil;

public class LogNoReduceFormatMR extends Configured implements Tool {

	public static class LogNoReduceFormatMappper extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		private LogNoReduceFormat logNoReduceFormat;
		private MultipleOutputs<Text, NullWritable> multipleOutputs;
		private Text outKeyTxt;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
			outKeyTxt = new Text();
			try {
				BeanFactoryUtil beanFactoryUtil = new BeanFactoryUtil(context
						.getConfiguration().get("configfilepath"));
				logNoReduceFormat = new LogNoReduceFormat();
				logNoReduceFormat.setBeanFactoryUtil(beanFactoryUtil);
				logNoReduceFormat.setContext(context);
				multipleOutputs = new MultipleOutputs<Text, NullWritable>(
						context);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String originLog = value.toString();

			try {
				outKeyTxt.clear();
				String formatLog = logNoReduceFormat.formatLog(originLog);
				outKeyTxt.set(formatLog);
				context.write(outKeyTxt, NullWritable.get());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				multipleOutputs.write(new Text(e.getMessage() + "\t"
						+ originLog), NullWritable.get(), "_error/part");
				return;
			}

		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJarByClass(LogNoReduceFormatMR.class);
		job.setMapperClass(LogNoReduceFormatMappper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		String jobName = job.getConfiguration().get(
				MapReduceConfInfoEnum.jobName.getValueStr());
		job.setJobName(jobName);
		String inputPathStr = job.getConfiguration().get(
				MapReduceConfInfoEnum.inputPath.getValueStr());
		System.out.println(inputPathStr);
		String outputPathStr = job.getConfiguration().get(
				MapReduceConfInfoEnum.outPutPath.getValueStr());
		System.out.println(outputPathStr);
		HdfsUtil.deleteDir(outputPathStr);
		int reduceNum = job.getConfiguration().getInt(
				MapReduceConfInfoEnum.reduceNum.getValueStr(), 0);
		System.out.println(MapReduceConfInfoEnum.reduceNum.getValueStr() + ":"
				+ reduceNum);
		FileInputFormat.setInputPaths(job, inputPathStr);
		FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
		job.setNumReduceTasks(reduceNum);
		int isInputLZOCompress = job
				.getConfiguration()
				.getInt(MapReduceConfInfoEnum.isInputFormatLZOCompress
						.getValueStr(),
						1);
		if (1 == isInputLZOCompress) {
			job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
		}
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(),
				new LogNoReduceFormatMR(), args);
		System.out.println(res);
		
	}
}
