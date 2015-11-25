package com.bi.mobilecoredata.middle.user.newactive;

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

import com.bi.mobilecoredata.middle.user.newactive.UserNewActiveListMR.UserNewActiveMap;
import com.bi.mobilecoredata.middle.user.newactive.UserNewActiveListMR.UserNewActiveReduce;



public class UserNewActiveList extends Configured implements Tool{

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		UserNewActiveListArgs userNewOrOldCountArgs = new UserNewActiveListArgs();
		int nRet = 0;

		try {
			userNewOrOldCountArgs.init("usernewactivelist.jar");
			userNewOrOldCountArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			// userNewOrOldCountArgs.parser.printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(),
				new UserNewActiveList(),
				userNewOrOldCountArgs.getUserNewActiveCountParam());
		System.out.println(nRet);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "UserNewActiveListMR");
		job.setJarByClass(UserNewActiveListMR.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(UserNewActiveMap.class);
		job.setReducerClass(UserNewActiveReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(8);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}
