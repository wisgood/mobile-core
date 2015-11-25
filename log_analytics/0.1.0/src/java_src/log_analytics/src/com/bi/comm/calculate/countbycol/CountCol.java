package com.bi.comm.calculate.countbycol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.calculate.countbycol.CountColMRUTL.CountColMapper;
import com.bi.comm.calculate.countbycol.CountColMRUTL.CountColReducer;

public class CountCol extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "CountColMRUTL");
		job.setJarByClass(CountCol.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().set("column", args[2]);
		job.getConfiguration().set("delim", args[3]);
		job.setMapperClass(CountColMapper.class);
		job.setCombinerClass(CountColReducer.class);
		job.setReducerClass(CountColReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(8);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		CountColArgs countArgs = new CountColArgs();
		int nRet = 0;

		try {
			countArgs.init("countbycol.jar");
			countArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			// countArgs.parser.printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(), new CountCol(),
				countArgs.getCountParam());
		System.out.println(nRet);

	}
}
