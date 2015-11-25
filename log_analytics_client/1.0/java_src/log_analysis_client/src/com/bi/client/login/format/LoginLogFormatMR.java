package com.bi.client.login.format;
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
import com.bi.client.util.*;
//import com.hadoop.compression.lzo.LzopCodec;
//import com.hadoop.compression.lzo.LzoIndexer;

public class LoginLogFormatMR extends Configured implements Tool {

	public static class LoginLogFormatMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		private Text      newValue  = null;
		private IpParser  ipParser  = null;  
		private String errorOutputDir  = null;
		private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
		private String  statDate = null;
		public void setup(Context context) throws IOException, InterruptedException{
			newValue  = new Text();
			ipParser  = new IpParser();
			statDate = context.getConfiguration().get("statDate");
			try {
				ipParser.init("ip_table");
			} catch ( IOException e) {
				e.printStackTrace();
				System.exit(0);
			}
			
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			errorOutputDir  = "error/part";
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {				
				CSVParser parser = new CSVParser();
				String[] fields = parser.parseLine(value.toString());
				LoginLog loginLog = new LoginLog(fields.length); 
				loginLog.setIpParser(ipParser);
				loginLog.setStatDate(statDate);
				loginLog.setFields(fields);
				newValue.set(loginLog.toString());
				context.write(NullWritable.get(), newValue);
			} catch (Exception e) {
				//newValue.set(e.getMessage() + "\t" + value.toString());
				//multipleOutputs.write(NullWritable.get(), newValue, errorOutputDir);
			}
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException {
			multipleOutputs.close();
		}

	}
	
	public static class LoginLogFormatReducer extends 
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
        
        conf.set("statDate", conf.get("stat_date"));
        Job job = new Job(conf, conf.get("job_name"));        
        FileInputFormat.addInputPaths(job, conf.get("input_dir"));
        Path output = new Path(conf.get("output_dir"));
        FileOutputFormat.setOutputPath(job, output);
        output.getFileSystem(conf).delete(output, true);        
	
		job.setJarByClass(LoginLogFormatMR.class);
		job.setMapperClass(LoginLogFormatMapper.class);	
		job.setReducerClass(LoginLogFormatReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
//        TextOutputFormat.setCompressOutput(job, true);
//        TextOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
        
        int code = job.waitForCompletion(true) ? 0 : 1;
       		
//	    LzoIndexer lzoIndexer = new LzoIndexer(conf);
//	    lzoIndexer.index(new Path(conf.get("output_dir")));
	    
		return code;
	}

	public static void main(String[] args) throws Exception {
		int nRet = ToolRunner
				.run(new Configuration(), new LoginLogFormatMR(), args);
		System.out.println(nRet);
	}
}
