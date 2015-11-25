package com.bi.comprehensive.uinfy.user;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.mapreduce.LzoTextInputFormat;

public class IdentityMR extends Configured implements Tool {
	
	public static class IdentityMapper extends
	    Mapper<LongWritable, Text, NullWritable, Text>{
	    public  void map (LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException{
	        context.write(NullWritable.get(), value);
	    }
	}

	public static class IdentityReducer extends
	    Reducer <NullWritable, Text, NullWritable, Text> {
	    public  void reduce ( NullWritable key,  Iterable <Text> values, Context context)
           throws IOException, InterruptedException{
	        for(Text val : values)
	            context.write(NullWritable.get(), val);
	    }
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
		conf = optionparser.getConfiguration();

		Job job = new Job(conf, "IdentityMR");
		job.setJarByClass(IdentityMR.class);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
	    String tmpDir    = conf.get("input_dir") + "_tmp";
		Path tmpOut      = new Path(tmpDir);	    
		FileOutputFormat.setOutputPath(job, tmpOut);
		tmpOut.getFileSystem(conf).delete(tmpOut, true);

		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(IdentityReducer.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(conf.getInt("reduce_num", 1));

		int code = job.waitForCompletion(true) ? 0 : 1;
		return code;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new IdentityMR(), args);
		System.out.println(res);
	}
}
