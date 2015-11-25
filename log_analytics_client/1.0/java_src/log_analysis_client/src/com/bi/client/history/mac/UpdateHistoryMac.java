package com.bi.client.history.mac;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import com.hadoop.compression.lzo.LzoIndexer;
//import com.hadoop.compression.lzo.LzopCodec;
//import com.hadoop.mapreduce.LzoTextInputFormat;

public class UpdateHistoryMac extends Configured implements Tool {
	
	public static class UpdateHistoryMacMapper extends
	    Mapper<LongWritable, Text, Text, NullWritable>{
	    private Text newKey = new Text();
	    public void map (LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
	        String[] fields= value.toString().split("\t");
	        int column = 0;
	        if(fields.length != 1){
	             column = 3;
	        }
	        
	        if(fields.length == 2){
	            newKey.set(fields[0].substring(4).toUpperCase());
	        }else{
	            newKey.set(fields[column]);
	        }
	        context.write(newKey, NullWritable.get());
	    }    
	}
      
    public static class UpdateHistoryMacReducer extends
        Reducer <Text, NullWritable, Text, NullWritable> {
        public void reduce (Text key, Iterable <NullWritable> values, Context context)
            throws IOException, InterruptedException{
            context.write(key, NullWritable.get());
        }      
    }

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
		conf = optionparser.getConfiguration();
		
		Job job = new Job(conf, "Update History Mac ");
		job.setJarByClass(UpdateHistoryMac.class);
		
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		Path outputPath = new Path(conf.get("output_dir"));
		FileOutputFormat.setOutputPath(job, outputPath );
        outputPath.getFileSystem(conf).delete(outputPath, true);
        
        job.setMapperClass(UpdateHistoryMacMapper.class);
        job.setReducerClass(UpdateHistoryMacReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
//      TextOutputFormat.setCompressOutput(job, true);
//      TextOutputFormat.setOutputCompressorClass(job, LzopCodec.class);       
        job.setNumReduceTasks(5);
        
        int code = job.waitForCompletion(true) ? 0 : 1 ;       
//      LzoIndexer lzoIndexer = new LzoIndexer(conf);
//      lzoIndexer.index(outputPath);        

        return code;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new UpdateHistoryMac(), args);
		System.out.println(res);
	}
}
