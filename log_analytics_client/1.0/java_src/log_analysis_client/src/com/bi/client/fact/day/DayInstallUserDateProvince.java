/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: DayInstallUserDateProvince.java 
* @Package com.bi.client.user.day 
* @Description: this class is to get the following index:
*               install_num, effective_install_num
* @author wang haiqiang
* @date 2013-8-10
* @input: boot:  /dw/logs/client/format/install
*         hs_log: /dw/logs/client/format/hs_log
* @output: f_client_day_instl_user_date_province:  /dw/logs/3_client/2_user/2_day/
* @executeCmd:hadoop jar UserDay.jar  com.bi.client.user.day.DayInstallUserDateProvince \
*                    /dw/logs/client/format/install/2013/08/01 \
*                    /dw/logs/client/format/hs_log//2013/08/01 \
*                    /dw/logs/3_client/2_user/2_day/f_client_day_instl_user_date_province/2013/08/01 \
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
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class DayInstallUserDateProvince extends Configured implements Tool {
   // private static final String SEPARATOR = "\t";

    /**
     * 
     */

//DayInstallUserDateProvince get the effective install user list 
    public static class DayInstallUserDateProvinceMapper extends
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
            StringBuilder strKey=new StringBuilder();
        	String [] strTemp= value.toString().split("\t");
        	
        	if(path.toString().contains("install"))
        	{
        	strKey.append(strTemp[3]+"\t"+strTemp[0]+"\t"+strTemp[2]);
        	
        	strValue.append("1");
        	
        	context.write(new Text(strKey.toString()), new Text(strValue.toString()));
        	} else 
        		if (path.toString().contains("hs_log")&&strTemp.length==20){
        			
        			//if(Integer.parseInt(strTemp[19])>20)
        			//{
        			strKey.append(strTemp[3]+"\t"+strTemp[0]+"\t"+strTemp[2]);
                	
        			strValue.append("2"+"\t"+strTemp[19]);
                	
                	context.write(new Text(strKey.toString()), new Text(strValue.toString()));
        			//}
			}

    }
    }

    public static class DayInstallUserDateProvinceReducer extends

            Reducer<Text, Text, Text, Text> {
    	

       

		

		protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
			String[] strTmpString=null;
			String[] strKeyString=null;

			Integer downloadnum=0;
			strKeyString=key.toString().split("\t");
			
		       Set<String> vHS = new HashSet<String>();
		     	
		     	for (Text value: values)
		     	{
		     		strTmpString=value.toString().split("\t");
		     		if(strTmpString[0].equals("2"))
		     		{
		     		downloadnum=downloadnum+Integer.parseInt(strTmpString[1]);
		     		}
		     		
		     		vHS.add(strTmpString[0]);
		     	}
		     	
		     	if (vHS.size()>1&&downloadnum>20)
		    	{
		     		context.write(new Text(strKeyString[1]+"\t"+strKeyString[2]), new Text(strKeyString[0]));
		    	}

    }
    }
    
    

    public static class DayInstallUserDateProvinceResultMapper extends
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
            StringBuilder strKey=new StringBuilder();
        	String [] strTemp= value.toString().split("\t");
        	
        	if(path.toString().contains("install"))
        	{
        	strKey.append(strTemp[0]+"\t"+strTemp[2]);
        	
        	strValue.append("1");
        	strValue.append("\t");
        	strValue.append("0");
        	
        	context.write(new Text(strKey.toString()), new Text(strValue.toString()));
           
        	} else 
        	{
        			
        			
        			strKey.append(strTemp[0]+"\t"+strTemp[1]);
                	
        			strValue.append("0");
                	strValue.append("\t");
                	strValue.append("1");
                	
                	context.write(new Text(strKey.toString()), new Text(strValue.toString()));
        			
			}

    }
    }

    public static class DayInstallUserDateProvinceResultReducer extends

            Reducer<Text, Text, Text, Text> {
    	

       

		

		protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
			String[] strTmpString=null;
			StringBuilder strTempBuilder=new StringBuilder();
			Integer intinstallInteger=0;
			Integer inteffectiveinstallInteger=0;
		      
		     	
		     	for (Text value: values)
		     	{
		     		strTmpString=value.toString().split("\t");
		     		intinstallInteger=intinstallInteger+Integer.parseInt(strTmpString[0]);
		     		inteffectiveinstallInteger=inteffectiveinstallInteger+Integer.parseInt(strTmpString[1]);
		     		
		     	}
		     	strTempBuilder.append(intinstallInteger.toString());
		     	strTempBuilder.append("\t");
		     	strTempBuilder.append(inteffectiveinstallInteger.toString());
		     
		     		context.write(key,new Text(strTempBuilder.toString()));

    }
    }
    
    
    
    
    
    
    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
       
    	int res = ToolRunner.run(new Configuration(),
				new DayInstallUserDateProvince(), args);
		System.out.println(res);

    }
    

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "DayInstallUserDateProvince");
        job.setJarByClass(DayInstallUserDateProvince.class);
       
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]+"tmp"));
        
        FileSystem.get(conf).delete(new Path(args[2]+"tmp"), true); 
        
        job.setMapperClass(DayInstallUserDateProvinceMapper.class);
        job.setReducerClass(DayInstallUserDateProvinceReducer.class);
        
        //job.setInputFormatClass(LzoTextInputFormat.class);
        
		job.setOutputFormatClass(TextOutputFormat.class);
		
        job.setNumReduceTasks(10);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        int code=job.waitForCompletion(true) ? 0 : 1;
        
        if(code==0)
        {
        	Job resultJob=new Job(conf, "DayInstallUserDateProvinceResult");
        	
        	resultJob.setJarByClass(DayInstallUserDateProvince.class);

        	  FileInputFormat.addInputPath(resultJob, new Path(args[0]));
              FileInputFormat.addInputPath(resultJob, new Path(args[2]+"tmp"));
              FileOutputFormat.setOutputPath(resultJob, new Path(args[2]));
              
              FileSystem.get(conf).delete(new Path(args[2]), true); 
              
        	resultJob.setMapperClass(DayInstallUserDateProvinceResultMapper.class);
        	resultJob.setReducerClass(DayInstallUserDateProvinceResultReducer.class);
        	
        	resultJob.setNumReduceTasks(10);
        	
        	resultJob.setMapOutputKeyClass(Text.class);
        	resultJob.setMapOutputValueClass(Text.class);
        	
        	resultJob.setOutputKeyClass(Text.class);
        	resultJob.setOutputValueClass(Text.class);
        	
        	code=resultJob.waitForCompletion(true)? 0 : 1;
        	
        	       	
        }
        Path tmpPath=new Path(args[2]+"tmp");
        FileSystem.get(conf).delete(tmpPath, true);    
              
        System.exit(code);
        
        return code;
    }
}