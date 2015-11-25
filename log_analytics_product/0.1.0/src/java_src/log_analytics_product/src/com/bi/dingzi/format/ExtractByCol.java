package com.bi.dingzi.format;

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

import com.bi.dingzi.format.ExtractByColMRUTL.ExtractMapper;
import com.bi.dingzi.format.ExtractByColMRUTL.ExtractReduce;




public class ExtractByCol extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("column", args[2]);
		conf.set("orderbycolum", args[3]);
		 conf.set("delim", args[4]);
		Job job = new Job(conf, "DingziExtractByCol");
		job.setJarByClass(ExtractByCol.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(ExtractByColMRUTL.ExtractMapper.class);
		job.setMapperClass(ExtractMapper.class);
		job.setReducerClass(ExtractReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(8);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		ExtractByColArgs  extractArgs  = new ExtractByColArgs();
		int nRet = 0;
		
		try{
			extractArgs.init("extractbycol.jar");
			extractArgs.parse(args);
		}
		catch(Exception e){
			System.out.println(e.toString());
			//extractArgs.parser.printUsage();
			System.exit(1);
		}
		
		nRet = ToolRunner.run(new Configuration(), new ExtractByCol(), extractArgs.getExtractParam());
		System.out.println(nRet);

	}
}

