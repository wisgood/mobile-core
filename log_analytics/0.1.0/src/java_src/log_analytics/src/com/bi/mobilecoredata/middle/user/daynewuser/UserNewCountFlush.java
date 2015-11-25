package com.bi.mobilecoredata.middle.user.daynewuser;

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

import com.bi.mobilecoredata.middle.user.daynewuser.UserNewCountFlushMR.UserCountMap;
import com.bi.mobilecoredata.middle.user.daynewuser.UserNewCountFlushMR.UserCountReduce;



public class UserNewCountFlush extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		UserNewCountFlushArgs userNewCountArgs = new UserNewCountFlushArgs();
		int nRet = 0;

		try {
			userNewCountArgs.init("usernewcountflush.jar");
			userNewCountArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			// userNewOrOldCountArgs.parser.printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(),
				new UserNewCountFlush(),
				userNewCountArgs.getUserNewOrOldCountFlushParam());
		System.out.println(nRet);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "UserNewCountFlushMR");
		job.setJarByClass(UserNewCountFlushMR.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(UserCountMap.class);
		job.setReducerClass(UserCountReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(4);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}
