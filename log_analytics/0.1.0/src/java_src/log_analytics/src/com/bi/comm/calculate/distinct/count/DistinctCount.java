package com.bi.comm.calculate.distinct.count;

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

import com.bi.comm.calculate.distinct.count.DistinctCountMRUTL.DistinctCountMapper;
import com.bi.comm.calculate.distinct.count.DistinctCountMRUTL.DistinctCountReduce;



public class DistinctCount extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("column", args[2]);
		//conf.set("mapred.job.tracker", "cluster-01:18001");

		Job job = new Job(conf, "DistinctCount");
		job.setJarByClass(DistinctCount.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(DistinctCountMapper.class);

		job.setReducerClass(DistinctCountReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}


	public static void main(String[] args) throws Exception {

		DistinctCountArgs  distinctArgs  = new DistinctCountArgs();
		int nRet = 0;
			
		try{
			distinctArgs.init("distinctcount.jar");
			distinctArgs.parse(args);
		}
		catch(Exception e){
			System.out.println(e.toString());
			System.exit(1);
		}
		
		nRet = ToolRunner.run(new Configuration(), new DistinctCount(), distinctArgs.getDistinctParam());
		System.out.println(nRet);

	}
}

