package com.bi.baidubrower.calculate;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.baidubrower.format.BrowerLog;

public class BaiduBrowerCalcMR extends Configured implements Tool {
    
    public static long getBetweenDays(String date1, String date2) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date firstDate = sdf.parse(date1);
        Date secondDate = sdf.parse(date2);
        long betweenDays = (firstDate.getTime() - secondDate.getTime()) / (1000 * 86400) ;
        betweenDays = betweenDays < 0 ? 1 : betweenDays + 1;
        if (betweenDays > 7)
             throw new ParseException("Wrong period",-1);
        return betweenDays;
    }

	public static class BaiduBrowerCalcMapper extends
			Mapper<LongWritable, Text, Text, Text> {
	    
	    private String   date = null;
	    private Text newValue = new Text();
	    private Text   newKey = new Text();	  
	    private String minDate = "";
	    private String statDate = "";
 	    public void setup(Context context) throws IOException, InterruptedException{
	        String[] pathNames = ((FileSplit) context.getInputSplit()).getPath()
                                    .getParent().toUri().getRawPath().split("/");
            date = pathNames[pathNames.length - 3] + pathNames[pathNames.length - 2] + pathNames[pathNames.length - 1];     
            minDate = context.getConfiguration().get("minDate");;
            statDate = context.getConfiguration().get("statDate");
 	    }
 	    
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {		 
		    String[] fields = value.toString().split("\t");
		    String ip = fields[BrowerLog.FormatLogOrder.IP.ordinal()];
		    String fck = fields[BrowerLog.FormatLogOrder.FCK.ordinal()];
		    String installDate  = fields[BrowerLog.FormatLogOrder.INSTALLDATE.ordinal()];
		    String timeDay = fields[BrowerLog.FormatLogOrder.TIMEDAY.ordinal()];
		    String compUser = fields[BrowerLog.FormatLogOrder.COMUSER.ordinal()];
		    
		    if( Integer.parseInt(installDate) < Integer.parseInt(minDate) 
		        || Integer.parseInt(installDate) > Integer.parseInt(statDate) )
		        return ;
    	    long betweenDays = 0;
    	    String secondDay = "-"; 
    	    String compDay = "-";
    	    try {
                betweenDays = getBetweenDays(date, installDate);
            }
            catch(ParseException e) {
                return ;
            }
    	   
    	    if(timeDay.equals("2"))
    	        secondDay = String.valueOf(betweenDays);
    	    if(compUser.equals("1"))
    	         compDay = String.valueOf(betweenDays);
    	    
    	    if(fck.equals("0"))
    	        newKey.set(ip + "," + installDate);
            else
                newKey.set(fck + "," + installDate);
    	    
    	    newValue.set(betweenDays + "," + secondDay + "," + compDay);	            
            context.write(newKey, newValue);
		}	
	}
	    
	public static class BaiduBrowerCalcReducer extends
			Reducer<Text, Text, NullWritable, Text> {
       private String statDate = "";
       private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
       private HashMap<String, ArrayList<Integer>> everyPushRecord = new HashMap<String, ArrayList<Integer>>();
       private ArrayList<Integer> secondPushRecord  = new ArrayList<Integer>(Arrays.asList(0,0,0,0,0,0,0));
       private ArrayList<Integer> compPushRecord    = new ArrayList<Integer>(Arrays.asList(0,0,0,0,0,0));
       private ArrayList<Integer> finalStatusRecord = new ArrayList<Integer>(Arrays.asList(0,0,0,0));
       
       public void setup(Context context) throws IOException, InterruptedException{
           statDate = context.getConfiguration().get("statDate");
           multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
       }
       
	   public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
	       int  secondDay = 8;
	       int  compDay   = 8; 
	       HashSet<Integer> pushDaySet = new HashSet<Integer>();
           
           for(Text val : values){
               String[] status = val.toString().split(",");
               pushDaySet.add(Integer.parseInt(status[0]));
               try {
               if(!status[1].equals("-")){
                   secondDay = Integer.parseInt(status[1]) < secondDay ? 
                               Integer.parseInt(status[1]) : secondDay;
               }
               if(!status[2].equals("-")){
                   compDay = Integer.parseInt(status[2]) < compDay ? 
                             Integer.parseInt(status[2]) : compDay;
               }  
               } catch (Exception e){
                    throw new IOException(e.getMessage() + ":" + val.toString());
               }
           }
           
           long betweenDays = 0;
           String installDate = key.toString().split(",")[1];
           try {
               betweenDays = getBetweenDays(statDate, installDate);
           }
           catch(ParseException e) {
               return;
           }
           
           if(betweenDays == 7){
               finalStatusRecord.set(0, Integer.parseInt(installDate));
               int index = 1;
               if(compDay != 8 && compDay > 2){
                   index = 3;
               }
               else if ( secondDay != 8  && secondDay > 1){
                   index = 2;
               }
               finalStatusRecord.set(index, finalStatusRecord.get(index) + 1);

               compPushRecord.set(0, Integer.parseInt(installDate));
               if(compDay != 8  && compDay > 2){
                   compPushRecord.set(compDay - 2, compPushRecord.get(compDay - 2) + 1); 
               }
                
               secondPushRecord.set(0, Integer.parseInt(installDate)); 
               if(compDay == 8  &&  secondDay != 8 && secondDay > 1 ){                
                   secondPushRecord.set(secondDay - 1 , secondPushRecord.get(secondDay - 1 ) + 1); 
               }   
           }
           
           if(! everyPushRecord.containsKey(installDate)){
               ArrayList<Integer> tmpList = new ArrayList<Integer>(Arrays.asList(0,0,0,0,0,0,0));
               everyPushRecord.put(installDate, tmpList);
           }
           
           for( int day : pushDaySet)
               everyPushRecord.get(installDate).set(day - 1, everyPushRecord.get(installDate).get(day - 1) + 1); 
	   }
       public void cleanup(Context context) throws IOException,InterruptedException {
           
           multipleOutputs.write(NullWritable.get(), new Text(joinArray(finalStatusRecord)), "F_BAIDU_BROWER_TOTAL");
           multipleOutputs.write(NullWritable.get(), new Text(joinArray(compPushRecord)), "F_BAIDU_BROWER_COMPLETE");
           multipleOutputs.write(NullWritable.get(), new Text(joinArray(secondPushRecord)), "F_BAIDU_BROWER_TWICE");
           for(String key : everyPushRecord.keySet()){
               multipleOutputs.write(NullWritable.get(), new Text(key + "\t" + joinArray(everyPushRecord.get(key))), "F_BAIDU_BROWER_ONCE");
           }
           multipleOutputs.close();
       }
       
       private String joinArray(ArrayList<Integer> list){
           StringBuilder str = new StringBuilder();
           String sep = "\t";
           for(Integer in : list){
               if(str.length() > 0)
                   str.append(sep);
               str.append(in);   
           }
           return str.toString();
       }
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
		conf = optionparser.getConfiguration();
		
		conf.set("statDate", conf.get("stat_date"));
		conf.set("minDate", conf.get("min_date"));
		Job job = new Job(conf, "BaiduBrowerCalcMR");
		job.setJarByClass(BaiduBrowerCalcMR.class);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
	    String outputDir = conf.get("output_dir");
	    Path outputPath  = new Path(outputDir);        
	    FileOutputFormat.setOutputPath(job, outputPath);
	    outputPath.getFileSystem(conf).delete(outputPath, true);

		job.setMapperClass(BaiduBrowerCalcMapper.class);
		job.setReducerClass(BaiduBrowerCalcReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
        int code = job.waitForCompletion(true) ? 0 : 1;
        return code;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new BaiduBrowerCalcMR(), args);
		System.out.println(res);
	}
}
