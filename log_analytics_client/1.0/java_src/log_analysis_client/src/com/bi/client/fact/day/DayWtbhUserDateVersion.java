/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: DayWtbhUserDateVersion.java 
* @Package com.bi.client.user.day 
* @Description: this class is to get the following index:
*               playusernum, playlength, playaveragelength, onlineaveragelength
* @author wang haiqiang
* @date 2013-8-10
* @input: boot:  /dw/logs/client/format/install
*         hs_log: /dw/logs/client/format/hs_log
* @output: f_client_day_wtbh_user_date_version:  /dw/logs/3_client/2_user/2_day/
* @executeCmd:hadoop jar UserDay.jar  com.bi.client.user.day.DayWtbhUserDateVersion \
*                    /dw/logs/client/format/hs_log//2013/08/01 \
*                    /dw/logs/client/format/wtbh/2013/08/01 \
*                    /dw/logs/3_client/2_user/2_day/f_client_day_wtbh_user_date_version/2013/08/01 \
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class DayWtbhUserDateVersion extends Configured implements Tool {
   // private static final String SEPARATOR = "\t";

    /**
     * 
     */
	

//DayWtbhUserDateVersion get the effective install user list 
    public static class DayWtbhUserDateVersionMapper extends
            Mapper<LongWritable, Text, Text, Text> {

       private Path path;
       
       
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            path = fileSplit.getPath();

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuilder strValue= new StringBuilder();
            StringBuilder strKey= new StringBuilder();
        	String[] strTemp= value.toString().split("\t");
        	

        	
        	 if(path.toString().contains("hs_log"))
         	{
        	
	        		strKey.append(strTemp[0]);
	        		strKey.append("\t");
	        		strKey.append(strTemp[1]);
	        		
	        		strValue.append("1");
	        		strValue.append("\t");
	        		strValue.append(strTemp[3]);
	        		
	        		
	        		context.write(new Text(strKey.toString()), new Text(strValue.toString()));
	        		
	        	

         	} else {
				
	        		strKey.append(strTemp[0]);
	        		strKey.append("\t");
	        		strKey.append(strTemp[1]);
	        		
	        		strValue.append("2");
	        		strValue.append("\t");
	        		strValue.append(strTemp[3]);
	        		strValue.append("\t");
	        		strValue.append(strTemp[5]);
	        		strValue.append("\t");
	        		
	        		strValue.append(strTemp[15]);
	        		context.write(new Text(strKey.toString()), new Text(strValue.toString()));
	        	
	        	
			}

    }
    }

    public static class DayWtbhUserDateVersionReducer extends

            Reducer<Text, Text, Text, Text> {
    	

       

		

		protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
			
			Integer onlineusernum=0;
			Integer playusernumInteger=0;
			Long playlength=(long) 0;
			Long onlineaveragelengthInteger=(long) 0;
			Long playaveragelengthInteger=(long) 0;
			StringBuilder valueStringBuilder=new StringBuilder();
			String[] strValueStrings=null;
			Long playtempLong=(long) 0;
			   Set<String> hslogmacSet=new HashSet<String>();
		       Set<String> wtbhmacSet=new HashSet<String>();
			
		     	
		     	for (Text value: values)
		     	{
		     		strValueStrings=value.toString().split("\t");
		     		
		     		if (strValueStrings[0].equals("1"))
		     		{
		     			if(!hslogmacSet.contains(strValueStrings[1]))
		     			{
		     				hslogmacSet.add(strValueStrings[1]);
		     			    onlineusernum=onlineusernum+1;
		     			}
		     		
		     		} else if (strValueStrings[0].equals("2"))
		     		{
		     			if(!wtbhmacSet.contains(strValueStrings[1]))
		     			{
		     				wtbhmacSet.add(strValueStrings[1]);
		     			    playusernumInteger=playusernumInteger+1;
			     		
			     		    
		     			}
		     			playtempLong=Long.parseLong(strValueStrings[2])-Long.parseLong(strValueStrings[3]);
		     			
		     			if(playtempLong>86400)
		     			{
		     			 playlength=playlength+86400;
		     			} else if (playtempLong>0)
		     			{
		     				playlength=playlength+playtempLong;
		     			}
		     		}
		     			
		     		
		     		
		     	}
		     	
		     	if (!onlineusernum.equals(0)&&!playusernumInteger.equals(0))
		     	{
		     	onlineaveragelengthInteger=playlength/onlineusernum;
		     	playaveragelengthInteger=playlength/playusernumInteger;
		        
		     	playlength=playlength/36000000;
		     	valueStringBuilder.append(playusernumInteger.toString());
		     	valueStringBuilder.append("\t");
		     	valueStringBuilder.append(playlength.toString());
		     	valueStringBuilder.append("\t");
		     	valueStringBuilder.append(playaveragelengthInteger.toString());
		     	valueStringBuilder.append("\t");
		     	valueStringBuilder.append(onlineaveragelengthInteger.toString());
		     	
		     	
		     	context.write(key, new Text(valueStringBuilder.toString()));
		    	
		     	}

    }
    }

    
    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
       
    	int res = ToolRunner.run(new Configuration(),
				new DayWtbhUserDateVersion(), args);
		System.out.println(res);

    }
    

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "DayWtbhUserDateVersion");
        job.setJarByClass(DayWtbhUserDateVersion.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
   	    FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        FileSystem.get(conf).delete(new Path(args[2]), true);
        
        job.setMapperClass(DayWtbhUserDateVersionMapper.class);
        job.setReducerClass(DayWtbhUserDateVersionReducer.class);
        
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