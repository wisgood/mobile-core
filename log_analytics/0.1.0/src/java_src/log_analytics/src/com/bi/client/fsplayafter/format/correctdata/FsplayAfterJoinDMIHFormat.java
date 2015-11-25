package com.bi.client.fsplayafter.format.correctdata;

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

import com.bi.client.fsplayafter.format.correctdata.FsplayAfterJoinDMIHFormatMR.FsplayAfterJoinDMIHFormatMap;
import com.bi.client.fsplayafter.format.correctdata.FsplayAfterJoinDMIHFormatMR.FsplayAfterJoinDMIHFormatReduce;

public class FsplayAfterJoinDMIHFormat extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		FsplayAfterJoinDMIHFormatArgs downloadJoinDMIHETLArgs = new FsplayAfterJoinDMIHFormatArgs();
		int nRet = 0;

		try {
			downloadJoinDMIHETLArgs.init("FsplayAfterjoindmih.jar");
			downloadJoinDMIHETLArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			// countArgs.parser.printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(), new FsplayAfterJoinDMIHFormat(),
				downloadJoinDMIHETLArgs.getParamStrs());
		System.out.println(nRet);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = new Job(conf, "FsplayAfterJoinDMIHFormatMR");
		job.setJarByClass(FsplayAfterJoinDMIHFormat.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(FsplayAfterJoinDMIHFormatMap.class);
		job.setReducerClass(FsplayAfterJoinDMIHFormatReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}
