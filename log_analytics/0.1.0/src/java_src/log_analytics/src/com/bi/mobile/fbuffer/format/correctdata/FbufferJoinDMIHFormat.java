package com.bi.mobile.fbuffer.format.correctdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.mobile.fbuffer.format.correctdata.FbufferJoinDMIHFormatMR.FbufferJoinDMIHFormatMap;
import com.bi.mobile.fbuffer.format.correctdata.FbufferJoinDMIHFormatMR.FbufferJoinDMIHFormatReduce;

public class FbufferJoinDMIHFormat extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		FbufferJoinDMIHFormatArgs downloadJoinDMIHETLArgs = new FbufferJoinDMIHFormatArgs();
		int nRet = 0;

		try {
			downloadJoinDMIHETLArgs.init("fbufferjoindmih.jar");
			downloadJoinDMIHETLArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			// countArgs.parser.printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(), new FbufferJoinDMIHFormat(),
				downloadJoinDMIHETLArgs.getParamStrs());
		System.out.println(nRet);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = new Job(conf, "FbufferJoinDMIHFormatMR");
		job.setJarByClass(FbufferJoinDMIHFormat.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(FbufferJoinDMIHFormatMap.class);
		job.setReducerClass(FbufferJoinDMIHFormatReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(4);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}
