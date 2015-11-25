package com.bi.mobile.exit.format.errordata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.mobile.exit.format.errordata.ExitFormatFilterByErrorMR.ExitFormatFilterByErrorMap;
import com.bi.mobile.exit.format.errordata.ExitFormatFilterByErrorMR.ExitFormatFilterByErrorReduce;
import com.bi.mobile.logs.format.param.LogArgs;

public class ExitFormatFilterByError extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// System.out.println("****************MainExitETLMR run print args*********************");
		// for(int i=0;i<args.length;i++){
		// System.out.println(args[i]);
		// }
		Configuration conf = getConf();
		Job job = new Job(conf, "ExitFormatFilterByErrorMR");
		job.setJarByClass(ExitFormatFilterByError.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(ExitFormatFilterByErrorMap.class);
		job.setReducerClass(ExitFormatFilterByErrorReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(8);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
	
		LogArgs logArgs = new LogArgs();
		int nRet = 0;
		try {
			logArgs.init("exitetlvalidate.jar");
			logArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(),
				new ExitFormatFilterByError(), logArgs.getParamStrs());
		System.out.println(nRet);
	}

}
