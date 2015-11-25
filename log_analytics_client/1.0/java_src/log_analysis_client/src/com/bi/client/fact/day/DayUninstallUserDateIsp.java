/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: DayUninstallUserDateIsp.java 
* @Package com.bi.client.user.day 
* @Description: this class is to get the following index:
*               uninstallnum
* @author wang haiqiang
* @date 2013-8-10
* @input: boot:  /dw/logs/client/format/uninstall
* @output: f_client_day_uninstl_user_date_isp:  /dw/logs/3_client/2_user/2_day/
* @executeCmd:hadoop jar UserDay.jar  com.bi.client.user.day.DayUninstallUserDateIsp \
*                    /dw/logs/client/format/uninstall/2013/08/01 \
*                    /dw/logs/3_client/2_user/2_day/f_client_day_uninstl_user_date_isp/2013/08/01 \
* @inputFormat:text
* @ouputFormat:text
*/
package com.bi.client.fact.day;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class DayUninstallUserDateIsp extends Configured implements Tool {
   // private static final String SEPARATOR = "\t";

    /**
     * 
     */
	

//DayUninstallUserDateIsp get the effective install user list 
    public static class DayUninstallUserDateIspMapper extends
            Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {


        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuilder strValue= new StringBuilder();
            StringBuilder strKey=new StringBuilder();
        	String [] strTemp= value.toString().split("\t");
        	

        		strKey.append(strTemp[0]);
        		strKey.append("\t");
        		strKey.append(strTemp[4]);
        		strValue.append(strTemp[3]);
        		context.write(new Text(strKey.toString()), new Text(strValue.toString()));
       	
        	
        	
    }
    }

    public static class DayUninstallUserDateIspReducer extends

            Reducer<Text, Text, Text, Text> {
    	

       

		

		protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
			
			
			Integer uninstallnum=0;
			
			Set<String> macSet=new HashSet<String>();  
		     	
		     	for (Text value: values)
		     	{
		     		if(!macSet.contains(value.toString()))
		     				{
		     			        macSet.add(value.toString());
		     					uninstallnum=uninstallnum+1;
		     				}
		        }
		     	
		     
		     		context.write(key, new Text(uninstallnum.toString()));

			
    }
    }
    

    
    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
       
    	int res = ToolRunner.run(new Configuration(),
				new DayUninstallUserDateIsp(), args);
		System.out.println(res);

    }
    

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "DayUninstallUserDateIsp");
        job.setJarByClass(DayUninstallUserDateIsp.class);
       

        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        FileSystem.get(conf).delete(new Path(args[1]), true); 
        
        job.setMapperClass(DayUninstallUserDateIspMapper.class);
        job.setReducerClass(DayUninstallUserDateIspReducer.class);
        
        //job.setInputFormatClass(LzoTextInputFormat.class);
        
		job.setOutputFormatClass(TextOutputFormat.class);
		
        job.setNumReduceTasks(10);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        int code=job.waitForCompletion(true) ? 0 : 1;
        
               
              
        System.exit(code);
        
        return code;
    }
}