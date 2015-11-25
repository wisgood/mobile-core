package com.bi.website.videoplay.format.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.website.videoplay.format.mr.WatchVVPartFormatMR.WatchVVPartFormatMap;
import com.bi.website.videoplay.format.mr.WatchVVPartFormatMR.WatchVVPartFormatReduce;

public class WatchVVPartFormat extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		conf.set("flags", args[2]);
		Job job = new Job(conf, "WatchVVPartFormat");
		job.setJarByClass(WatchVVPartFormatMR.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
		job.setMapperClass(WatchVVPartFormatMap.class);
		job.setReducerClass(WatchVVPartFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(6);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		WatchVVArgs logArgs = new WatchVVArgs();
		int nRet = 0;
		try {
			logArgs.init("WatchVVPartFormat.jar");
			logArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		 }

		nRet = ToolRunner.run(new Configuration(), new WatchVVPartFormat(),
				logArgs.getCountParam());
		System.out.println(nRet);
	}

}
