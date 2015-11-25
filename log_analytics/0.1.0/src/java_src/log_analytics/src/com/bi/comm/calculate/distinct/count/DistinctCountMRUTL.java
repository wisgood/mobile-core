package com.bi.comm.calculate.distinct.count;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DistinctCountMRUTL {

	public static class DistinctCountMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {
		private HashMap<String,String> hashmap = new HashMap<String,String>();
		private String[] colNum = null;

		public void setup(Context context) {
			colNum = context.getConfiguration().get("column").split(",");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
	        String colKey = "";
			String[] field = value.toString().trim().split("\t");
           			
			for (int i = 0; i < colNum.length; i++) {			
				colKey +=  field[Integer.parseInt(colNum[i])] + "\t";
			}
			
			if (! hashmap.containsKey(colKey) ){
				hashmap.put(colKey, "1");
				context.write(NullWritable.get(), new Text(colKey));
			}
								
		}
	}

	public static class DistinctCountReduce extends
			Reducer<NullWritable, Text, NullWritable, Text> {
		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException  {
			HashMap<String,String> hashmap = new HashMap<String,String>();
			int count = 0;
			for (Text val : values) {
	            String tmpVal = val.toString();
	            if (! hashmap.containsKey(tmpVal)){
	            	hashmap.put(tmpVal, "1");
	            	count ++;
	            }
			}	        	    
			context.write(NullWritable.get(), new Text(String.valueOf(count)));
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		String column = "7";
		String distinctBootInput = "output_fbuffer";
		String distinctTmpOutput = "output_user_day_watch";
    	Job job = new Job();
		job.setJarByClass(DistinctCount.class);
		job.setJobName("DistinctCountMRUTL");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		Configuration conf = job.getConfiguration();
		conf.set("column", column);
		FileInputFormat.setInputPaths(job, new Path(distinctBootInput));
		FileOutputFormat.setOutputPath(job, new Path(distinctTmpOutput));
		job.setMapperClass(DistinctCountMapper.class);
		job.setReducerClass(DistinctCountReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
