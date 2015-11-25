/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: DayBootUserDate.java 
* @Package com.bi.client.user.day 
* @Description: this class is to get the different type of boot num, as following index:
*               boot_num,boot_0_num,boot_1_num,boot_2_num,boot_3_num
* @author wang haiqiang
* @date 2013-8-10
* @input: boot:  /dw/logs/client/format/boot
* @output: f_client_day_boot_user_date:  /dw/logs/3_client/2_user/2_day/
* @executeCmd:hadoop jar UserDay.jar  com.bi.client.user.day.DayBootUserDate \
*                    /dw/logs/client/format/boot/2013/08/01 \
*                    /dw/logs/3_client/2_user/2_day/f_client_day_boot_user_date/2013/08/01 \
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
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class DayBootUserDate extends Configured implements Tool {
   // private static final String SEPARATOR = "\t";

    /**
     * 
     */


    public static class DayBootUserDateMapper extends
            Mapper<LongWritable, Text, Text, Text> {

      // private Path path;
      
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
        	
        	strValue.append(strTemp[3]);
        	strValue.append("\t");
        	
        	if (strTemp[5].equals("0"))
        	{
        		strValue.append("0");
        		strValue.append("\t");
        		strValue.append(strTemp[3]);
        		
        	} else if (strTemp[5].equals("1"))
        	{
        		strValue.append("1");
        		strValue.append("\t");
        		strValue.append(strTemp[3]);
        		
        	} else if (strTemp[5].equals("2"))
        	{
        		strValue.append("2");
        		strValue.append("\t");
        		strValue.append(strTemp[3]);
        		
        	} else if (strTemp[5].equals("3"))
        	{
        		strValue.append("3");
        		strValue.append("\t");
        		strValue.append(strTemp[3]);
        	}
        	
        	
            
        	context.write(new Text(strKey.toString()), new Text(strValue.toString()));

    }
    }

    public static class DayBootUserDateReducer extends
            Reducer<Text, Text, Text, Text> {
    	

    	

		

		protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
			
			StringBuilder strTempBuilder=new StringBuilder();
			String[] strValueStrings=null;
			Integer intSumInteger=0;
			Integer intSumInteger_0=0;
			Integer intSumInteger_1=0;
			Integer intSumInteger_2=0;
			Integer intSumInteger_3=0;
			
			Set<String> macSet=new HashSet<String>();
			Set<String> macSet_0=new HashSet<String>();
			Set<String> macSet_1=new HashSet<String>();
			Set<String> macSet_2=new HashSet<String>();
			Set<String> macSet_3=new HashSet<String>();
			
			for(Text val : values)
          		{
				
				
				strValueStrings=val.toString().split("\t");
				
				
				if (!macSet.contains(strValueStrings[0]))
				{
					macSet.add(strValueStrings[0]);
				    intSumInteger=intSumInteger+1;
				
				
				}
				
				if (strValueStrings[1].equals("0"))
				{
					if (!macSet_0.contains(strValueStrings[2]))
					{
						macSet_0.add(strValueStrings[2]);
					    intSumInteger_0=intSumInteger_0+1;
					
					
					}
					
				} else if (strValueStrings[1].equals("1"))
				{
					if (!macSet_1.contains(strValueStrings[2]))
					{
						macSet_1.add(strValueStrings[2]);
					
					    intSumInteger_1=intSumInteger_1+1;

					
					}
				} else if (strValueStrings[1].equals("2"))
				{
					if (!macSet_2.contains(strValueStrings[2]))
					{
						macSet_2.add(strValueStrings[2]);
					
					    intSumInteger_2=intSumInteger_2+1;
					
					
					}
				} else if (strValueStrings[1].equals("3"))
					if (!macSet_3.contains(strValueStrings[2]))
					{
						macSet_3.add(strValueStrings[2]);
					    intSumInteger_3=intSumInteger_3+1;
					
					}
				
          		}
			
			
			
			
			
			strTempBuilder.append(intSumInteger.toString()).append("\t");
			strTempBuilder.append(intSumInteger_0.toString()).append("\t");
			strTempBuilder.append(intSumInteger_1.toString()).append("\t");
			strTempBuilder.append(intSumInteger_2.toString()).append("\t");
			strTempBuilder.append(intSumInteger_3.toString());
			
			context.write(key, new Text(strTempBuilder.toString()));
    }
    }
    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
       
    	int res = ToolRunner.run(new Configuration(),
				new DayBootUserDate(), args);
		System.out.println(res);

    }
    

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "DayBootUserDate");
        job.setJarByClass(DayBootUserDate.class);
       
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        FileSystem.get(conf).delete(new Path(args[1]), true); 
        
        job.setMapperClass(DayBootUserDateMapper.class);
        job.setReducerClass(DayBootUserDateReducer.class);
        
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