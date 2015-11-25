package com.bi.baidubrower.format;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.bi.baidubrower.util.*;

public class BrowerLogFormatMR extends Configured implements Tool {

	public static class BrowerLogFormatMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		private Text      newValue  = new Text();
//		private String errorOutputDir  = null;
//		private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
//		public void setup(Context context) throws IOException, InterruptedException{			
//			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
//			errorOutputDir  = "error/part";
//		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {				
				CSVParser parser = new CSVParser();
				String[] fields = parser.parseLine(value.toString());
				BrowerLog BrowerLog = new BrowerLog(fields.length); 
				BrowerLog.setFileds(fields);
				newValue.set(BrowerLog.toString());
				context.write(NullWritable.get(), newValue);
			} catch (Exception e) {
//				newValue.set(e.getMessage() + "\t" + value.toString());
//				multipleOutputs.write(NullWritable.get(), newValue, errorOutputDir);
			}
		}
		
//		public void cleanup(Context context) throws IOException,InterruptedException {
//			multipleOutputs.close();
//		}
	}
	
	public static class BrowerLogFormatReducer extends 
	    Reducer <NullWritable, Text, NullWritable, Text> {
	    public void reduce(NullWritable Key, Iterable <Text> values, Context context) 
	            throws IOException,InterruptedException{
            for (Text val : values)
                context.write(NullWritable.get(), val);	        
	    }	        
	}

	public int run(String[] args) throws Exception {
	    Configuration conf = getConf();
        GenericOptionsParser gop = new GenericOptionsParser(conf, args);
        conf = gop.getConfiguration();

        Job job = new Job(conf, conf.get("job_name"));        
        FileInputFormat.addInputPaths(job, conf.get("input_dir"));
        Path output = new Path(conf.get("output_dir"));
        FileOutputFormat.setOutputPath(job, output);
        output.getFileSystem(conf).delete(output, true);        
	
		job.setJarByClass(BrowerLogFormatMR.class);
		job.setMapperClass(BrowerLogFormatMapper.class);	
		job.setReducerClass(BrowerLogFormatReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
    
        int code = job.waitForCompletion(true) ? 0 : 1;
		return code;
	}

	public static void main(String[] args) throws Exception {
		int nRet = ToolRunner
				.run(new Configuration(), new BrowerLogFormatMR(), args);
		System.out.println(nRet);
	}
}
