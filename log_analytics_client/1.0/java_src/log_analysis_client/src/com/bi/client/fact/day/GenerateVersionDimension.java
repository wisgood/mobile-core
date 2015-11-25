/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: DayWtbhUserDateVersion.java 
* @Package com.bi.client.user.day 
* @Description: this class is to get the version dimension
* @author wang haiqiang
* @date 2013-8-10
* @input: boot:  /dw/logs/client/format/install
*         hs_log: /dw/logs/client/format/hs_log
* @output: f_client_day_wtbh_user_date_version:  /dw/logs/3_client/2_user/2_day/
* @executeCmd:hadoop jar UserDay.jar  com.bi.client.user.day.GenerateVersionDimension \
*                    /dw/logs/client/format/hs_log/2013/08/01 \
*                    /dw/logs/3_client/2_user/5_dimension/dm_client_version/2013/08/01 \
* @inputFormat:text
* @ouputFormat:text
*/
package com.bi.client.fact.day;

import java.io.IOException;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;






public class GenerateVersionDimension extends Configured implements Tool {
   // private static final String SEPARATOR = "\t";

    /**
     * 
     */
	

    public static class GenerateVersionDimensionMapper extends
            Mapper<LongWritable, Text, Text, Text> {

      // private Path path;
      
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
          //  FileSplit fileSplit = (FileSplit) context.getInputSplit();
           // path = fileSplit.getPath();

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuilder strValue= new StringBuilder();
            StringBuilder strKey=new StringBuilder();
        	String [] strTemp= value.toString().split("\t");
        	
        	       	
        	long versionid=Long.parseLong(strTemp[1].toString());
        	String version=IpParser.longtoip(versionid);
        	
            strKey.append(strTemp[1]);
            strValue.append(version);

        	context.write(new Text(strKey.toString()), new Text(strValue.toString()));

    }
    }

    public static class GenerateVersionDimensionReducer extends
            Reducer<Text, Text, Text, Text> {
    	

    	

		

		protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
			
			
			String strValueStrings=null;
							
			for(Text val : values)
          		{
							
				strValueStrings=val.toString();
								
          		}

			context.write(key, new Text(strValueStrings.toString()));
    }
    }
    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
       
    	int res = ToolRunner.run(new Configuration(),
				new GenerateVersionDimension(), args);
		System.out.println(res);

    }
    

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "GenerateVersionDimension");
        job.setJarByClass(GenerateVersionDimension.class);
       
//        FileInputFormat.addInputPath(job, new Path("/user/db2inst1/test/hs_log/hs_log.copy"));
//        FileOutputFormat.setOutputPath(job, new Path("/user/db2inst1/output/versiondimension/date"));
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(GenerateVersionDimensionMapper.class);
        job.setReducerClass(GenerateVersionDimensionReducer.class);
        
       // job.setInputFormatClass(LzoTextInputFormat.class);
        
		job.setOutputFormatClass(TextOutputFormat.class);
		
        job.setNumReduceTasks(10);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}