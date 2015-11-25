package com.bi.mobilecoredata.middle.fbuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.mobilecoredata.middle.fbuffer.FbufferonditionMR.FbufferonditionMapper;
import com.bi.mobilecoredata.middle.fbuffer.FbufferonditionMR.FbufferonditionReduce;

public class FbufferCondition extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		FbufferConditionArgs fbufferConditionArgs = new FbufferConditionArgs();
		int nRet = 0;

		try {
			fbufferConditionArgs.init("fbuffercondition.jar");
			fbufferConditionArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			// FbufferConditionArgs.parser.printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(), new FbufferCondition(),
				fbufferConditionArgs.getFufferConditionParam());
		System.out.println(nRet);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(FbufferCondition.class);
		job.setJobName("FbufferConditionOKMR");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(FbufferonditionMapper.class);
		job.setReducerClass(FbufferonditionReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(4);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}
