package com.bi.log.pv.format.correctdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.log.pv.format.correctdata.PvFormatMR.PvFormatMap;
import com.bi.log.pv.format.correctdata.PvFormatMR.PvFormatReduce;
import com.bi.log.format.param.LogArgs;


public class PvFormat extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = getConf();
        Job job = new Job(conf, "PvFormatMR");
		job.setJarByClass(PvFormat.class);
		job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(PvFormatMap.class);
		job.setReducerClass(PvFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(60);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LogArgs  logArgs  = new LogArgs();
		int nRet = 0;
		try{
			logArgs.init("pv_format.jar");
			logArgs.parse(args);
		}
		catch(Exception e){
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		}
		
		nRet = ToolRunner.run(new Configuration(), new PvFormat(), logArgs.getParamStrs());
		System.out.println(nRet);
	}

}
