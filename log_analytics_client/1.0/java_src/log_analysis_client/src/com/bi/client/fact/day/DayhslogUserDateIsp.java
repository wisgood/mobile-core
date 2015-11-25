/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: DayhslogUserDateIsp.java 
* @Package com.bi.client.user.day 
* @Description: this class is to get the following index:
*               onlinenum,newnum,cutdownnum,withflownum,withoutflownum,onlinelength,onlineavearge
* @author wang haiqiang
* @date 2013-8-10
* @input: hs_log:  /dw/logs/client/format/hs_log
*         history_mac:  /dw/logs/3_client/2_user/1_history_mac
* @output: f_client_day_online_user_date_isp:  /dw/logs/3_client/2_user/2_day/
* @executeCmd:hadoop jar UserDay.jar  com.bi.client.user.day.DayhslogUserDateIsp \
*                    /dw/logs/client/format/hs_log/2013/08/01 \
*                    /dw/logs/3_client/2_user/1_history_mac/2013/08/01 \
*                    /dw/logs/3_client/2_user/2_day/f_client_day_online_user_date_isp/2013/08/01
* @inputFormat:...
* @ouputFormat:...
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





public class DayhslogUserDateIsp extends Configured implements Tool {
   // private static final String SEPARATOR = "\t";

    /**
     * 
     */

//DayhslogUserDateIsp get the effective install user list 
    public static class DayhslogUserDateIspMapper extends
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
        	
        	
        		String dateid=new String();
        		String mac=new String();
        		String ispid=new String();
        		String logintype=new String();
        		String starttime=new String();
        		String endtime=new String();
        		String upflow=new String();
        		String endflow=new String();
        		
        if(path.toString().contains("hs_log"))
        	{

        	 dateid=strTemp[0];
    		 mac=strTemp[3];
    		 ispid=strTemp[4];
    		 logintype=strTemp[10];
    		 starttime=strTemp[13];
    		 endtime=strTemp[14];
    		 upflow=strTemp[18];
    		 endflow=strTemp[19];
  	

        			strValue.append("1");
        			strValue.append("\t");
        			if(logintype.matches("^cutdown.*$"))
        			{
        				strValue.append("1");
            			strValue.append("\t");
        			} else {
        				strValue.append("0");
            			strValue.append("\t");
					}
        				
        			if(Integer.parseInt(endtime)>Integer.parseInt(starttime))
        			{ 
        				Integer tmplength=Integer.parseInt(endtime)-Integer.parseInt(starttime);
        				strValue.append(tmplength.toString());
            			strValue.append("\t");
        			}else {
        				strValue.append("0");
            			strValue.append("\t");
        			}
        			
        			if(Integer.parseInt(upflow)>0 || Integer.parseInt(endflow)>0)
        			{ 
        				
        				strValue.append("1");
        				strValue.append("\t");
        			}else {
        				strValue.append("0");
        				strValue.append("\t");
        			}
        			
        			strValue.append("0");
        			strValue.append("\t");
        			strValue.append(mac);
        			    
        			
        				strKey.append(dateid);
        				strKey.append("\t");
        				strKey.append(ispid);
        				
        				context.write(new Text(strKey.toString()), new Text(strValue.toString()));

        	} else {
				
        		strValue.append("0");
    			strValue.append("\t");
    			strValue.append("0");
    			strValue.append("\t");
    			strValue.append("0");
    			strValue.append("\t");
    			strValue.append("0");
    			strValue.append("\t");
    			strValue.append("1");
    			strValue.append("\t");
    			strValue.append("-");
        		context.write(new Text(strTemp[0]+"\t"+strTemp[1]), new Text(strValue.toString()));
        		
        		
			}
        	
        	
        	
    }
    }

    public static class DayhslogUserDateIspReducer extends

            Reducer<Text, Text, Text, Text> {
    	

       

		

		protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
			
			String[] strTmpString=null;
			
			StringBuilder strTempBuilder=new StringBuilder();
			

			
			Integer onlinenum=0;
			Integer cutdownnumInteger=0;
			Integer withflownum=0;
			Integer withoutflownum=0;
			Long  onlinelengh=(long) 0;
			Long newnum=(long) 0;
			Long onlineaverage=(long) 0;
				 
			
			Set<String> onlinemacSet=new HashSet<String>();
			Set<String> cutdownmacSet=new HashSet<String>();
			Set<String> withflowmacSet=new HashSet<String>();
		     	
		     	for (Text value: values)
		     	{
		     		strTmpString=value.toString().split("\t");
		     		if(strTmpString[0].equals("1"))
		     		{
		     			onlinemacSet.add(strTmpString[5]);
		     		}
		     		if(strTmpString[1].equals("1"))
		     		{
		     			cutdownmacSet.add(strTmpString[5]);
		     		}
		     		if(strTmpString[3].equals("1"))
		     		{
		     			withflowmacSet.add(strTmpString[5]);
		     		}
		     		onlinelengh=onlinelengh+Long.parseLong(strTmpString[2]);
		     		newnum=newnum+Long.parseLong(strTmpString[4]);
		     	}
		     	
		     	onlinenum=onlinemacSet.size();
		     	cutdownnumInteger=cutdownmacSet.size();
		     	withflownum=withflowmacSet.size();
		     	
		     	withoutflownum=onlinenum-withflownum;
		     	onlineaverage=onlinelengh/onlinenum;
		     	
		     	
		     	onlinelengh=onlinelengh/36000000;
		     	strTempBuilder.append(onlinenum.toString());
		     	strTempBuilder.append("\t");
		     	strTempBuilder.append(newnum.toString());
		     	strTempBuilder.append("\t");
		     	strTempBuilder.append(cutdownnumInteger.toString());
		     	strTempBuilder.append("\t");
		     	strTempBuilder.append(withflownum.toString());
		     	strTempBuilder.append("\t");
		     	strTempBuilder.append(withoutflownum.toString());
		     	strTempBuilder.append("\t");
		     	strTempBuilder.append(onlinelengh.toString());
		     	strTempBuilder.append("\t");
		     	strTempBuilder.append(onlineaverage.toString());
		     	
		        context.write(key,new Text(strTempBuilder.toString()));
		    	
	
			}
			
			
    }

    
    public static class DayhslogUserDateIspNewMacMapper extends
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
   
	String [] strTemp= value.toString().split("\t");
	
	
	
		
		if(path.toString().contains("hs_log"))
    	{
		
				context.write(new Text(strTemp[3]), new Text("1"+"\t"+strTemp[0]+"\t"+strTemp[4]));
	
	
			} else if(path.toString().contains("1_history_mac")){
				
				
				context.write(new Text(strTemp[0]), new Text("2"));
				
			}

}
}

    public static class DayhslogUserDateIspNewMacReducer extends

    Reducer<Text, Text, Text, Text> {






protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
    // TODO Auto-generated method stub
	
	String[] strTmpString=null;
	String strdateString=null;
	
       Set<String> vHS = new HashSet<String>();
     	
     	for (Text value: values)
     	{
     		strTmpString=value.toString().split("\t");
     		if(strTmpString[0].equals("1"))
     		{
     		strdateString=strTmpString[1]+"\t"+strTmpString[2];
     		}
     		vHS.add(strTmpString[0]);
     	}
     	
     	if (vHS.contains("1") && !vHS.contains("2"))
    	{  
     		context.write(new Text(strdateString),key);
    	}

		
	}
	
	
}


   
    

    public static void main(String[] args) throws Exception {
       
    	int res = ToolRunner.run(new Configuration(),
				new DayhslogUserDateIsp(), args);
		System.out.println(res);

    }
    

    @Override
    public int run(String[] args) throws Exception {

        // TODO Auto-generated method stub

    	
    	Configuration conf = getConf();

    	
        Job job = new Job(conf, "DayhslogUserDateIspNewMac");
        job.setJarByClass(DayhslogUserDateIsp.class);
            
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]+"tmp"));
        FileSystem.get(conf).delete(new Path(args[2]+"tmp"), true);
        job.setMapperClass(DayhslogUserDateIspNewMacMapper.class);
        job.setReducerClass(DayhslogUserDateIspNewMacReducer.class);
        
       // job.setInputFormatClass(LzoTextInputFormat.class);
        
		job.setOutputFormatClass(TextOutputFormat.class);
		
        job.setNumReduceTasks(10);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        int code=job.waitForCompletion(true) ? 0 : 1;
        
        if(code==0)
        {
        	Job resultJob=new Job(conf, "DayhslogUserDateIsp");
        	
        	resultJob.setJarByClass(DayhslogUserDateIsp.class);
        	
        	FileInputFormat.addInputPath(resultJob, new Path(args[0]));
            FileInputFormat.addInputPath(resultJob, new Path(args[2]+"tmp"));
            FileOutputFormat.setOutputPath(resultJob, new Path(args[2]));
            FileSystem.get(conf).delete(new Path(args[2]), true); 
        	
        	resultJob.setMapperClass(DayhslogUserDateIspMapper.class);
        	resultJob.setReducerClass(DayhslogUserDateIspReducer.class);
        	
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