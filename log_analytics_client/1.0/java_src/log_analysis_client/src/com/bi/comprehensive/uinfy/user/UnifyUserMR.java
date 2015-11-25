package com.bi.comprehensive.uinfy.user;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.lang.Enum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static com.bi.comprehensive.uinfy.user.UnifyUserUtil.*;
import static com.bi.comprehensive.uinfy.user.StringSplit.*;

public class UnifyUserMR extends Configured implements Tool {
    public static class UnifyUserMapper extends
        Mapper<LongWritable, Text, Text, Text> {
         
        private Text newValue = new Text();
        private Text newKey   = new Text();
        private int logType   = -1;
        private int macColumnId = -1;
        private char logSeperator = '\t';
        public void setup(Context context) throws IOException, InterruptedException {
            String path = ((FileSplit)context.getInputSplit()).getPath()
                    .getParent().toUri().getRawPath();
             System.out.println();
             logType = getLogType(path);
             if(logType == -1 )
                 System.exit(-1);
             macColumnId = LogType.values()[logType].getColumnId();
             logSeperator = LogType.values()[logType].getLogSeperator();
        }
        
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {           
            String[] fields = splitLog(value.toString(), logSeperator);
            newKey.set(fields[macColumnId - 1 ]);
            if(logType == LogType.PC_PV.ordinal()){
                
                if(fields[URL_COLUMN_ID  - 1].startsWith("http://fs")){
                    newValue.set("fs");
                }else if(fields[URL_COLUMN_ID - 1].startsWith("http://www")){
                    newValue.set("www");
                }else{
                    return;
                }
            }else if(logType == LogType.PC_BROWSER.ordinal()){
                if( !fields[SUC_COLUMN_ID -1 ].equals("1"))
                    return;
                newValue.set(logType + "");   
            }else{
                newValue.set(logType + "");
            }
            context.write(newKey, newValue);
       }
    }
    	
    public static class UnifyUserCombiner extends
        Reducer <Text, Text, Text, Text> {
        private HashSet<String> logTypeSet = new HashSet<String>();
        public void reduce(Text Key, Iterable <Text> values, Context context) 
            throws IOException,InterruptedException{
            logTypeSet.clear();
            for (Text val : values){
                  String[] fields = val.toString().split(",");
                  logTypeSet.addAll(Arrays.asList(fields));
            }
            String str = "";
            for(String log : logTypeSet){
                if(str.length() > 0)
                    str += ",";
                str += log;
            }
            context.write(Key, new Text(str));         
        }           
    }
		
	public static class UnifyUserReducer extends 
	    Reducer <Text, Text, Text, Text> {
	    private HashMap<Enum<UnifyUserIndex>, Integer> unifyUserIndexSet = 
	                              new HashMap<Enum<UnifyUserIndex>, Integer>();
	    private HashSet<String> logTypeSet = new HashSet<String>();
	    private Text newKey = new Text();
	    private Text newValue = new Text();
        public void setup(Context context) throws IOException, InterruptedException {
            for(UnifyUserIndex index : UnifyUserIndex.values())
                unifyUserIndexSet.put(index, 0);
        }
	    public void reduce(Text Key, Iterable <Text> values, Context context) 
	            throws IOException,InterruptedException{
	        logTypeSet.clear();
            for (Text val : values){
                    String[] fields = val.toString().split(",");
                    logTypeSet.addAll(Arrays.asList(fields));
            }
            
            // 总用户数（含工具屏保）
            if( logTypeSet.contains(String.valueOf(LogType.PC_BOOT.ordinal()))
             || logTypeSet.contains(String.valueOf(LogType.PC_AIRFIELD.ordinal()))
             || logTypeSet.contains(String.valueOf(LogType.PC_BROWSER.ordinal()))
             || logTypeSet.contains(String.valueOf(LogType.PC_SCREEN.ordinal())))
                unifyUserIndexSet.put(UnifyUserIndex.All_USER_WITH_TOOL,
                    unifyUserIndexSet.get(UnifyUserIndex.All_USER_WITH_TOOL) + 1);
            if(logTypeSet.contains("www") && ! logTypeSet.contains("fs"))
                unifyUserIndexSet.put(UnifyUserIndex.All_USER_WITH_TOOL,
                    unifyUserIndexSet.get(UnifyUserIndex.All_USER_WITH_TOOL) + 1);
            if( logTypeSet.contains(String.valueOf(LogType.MOBILE_BOOT.ordinal()))
             || logTypeSet.contains(String.valueOf(LogType.MOBILE_EXIT.ordinal())))
                unifyUserIndexSet.put(UnifyUserIndex.All_USER_WITH_TOOL,
                    unifyUserIndexSet.get(UnifyUserIndex.All_USER_WITH_TOOL) + 1);  
            if( logTypeSet.contains(String.valueOf(LogType.MOBILE_PV.ordinal())))
                unifyUserIndexSet.put(UnifyUserIndex.All_USER_WITH_TOOL,
                    unifyUserIndexSet.get(UnifyUserIndex.All_USER_WITH_TOOL) + 1); 
            
             // 总用户数（不含工具屏保）
            if( logTypeSet.contains(String.valueOf(LogType.PC_BOOT.ordinal())))
                unifyUserIndexSet.put(UnifyUserIndex.ALL_USER_WITHOUT_TOOL,
                    unifyUserIndexSet.get(UnifyUserIndex.ALL_USER_WITHOUT_TOOL) + 1);
            if( logTypeSet.contains("www") && ! logTypeSet.contains("fs"))
                unifyUserIndexSet.put(UnifyUserIndex.ALL_USER_WITHOUT_TOOL,
                    unifyUserIndexSet.get(UnifyUserIndex.ALL_USER_WITHOUT_TOOL) + 1);
            if( logTypeSet.contains(String.valueOf(LogType.MOBILE_BOOT.ordinal()))
                || logTypeSet.contains(String.valueOf(LogType.MOBILE_EXIT.ordinal())))
                unifyUserIndexSet.put(UnifyUserIndex.ALL_USER_WITHOUT_TOOL,
                    unifyUserIndexSet.get(UnifyUserIndex.ALL_USER_WITHOUT_TOOL) + 1);  
            if( logTypeSet.contains(String.valueOf(LogType.MOBILE_PV.ordinal())))
                unifyUserIndexSet.put(UnifyUserIndex.ALL_USER_WITHOUT_TOOL,
                    unifyUserIndexSet.get(UnifyUserIndex.ALL_USER_WITHOUT_TOOL) + 1); 
            
            // PC端总用户数（含工具屏保）
            if( logTypeSet.contains(String.valueOf(LogType.PC_BOOT.ordinal()))
             || logTypeSet.contains(String.valueOf(LogType.PC_AIRFIELD.ordinal()))
             || logTypeSet.contains(String.valueOf(LogType.PC_BROWSER.ordinal()))
             || logTypeSet.contains(String.valueOf(LogType.PC_SCREEN.ordinal())))
                unifyUserIndexSet.put(UnifyUserIndex.PC_CLIENT_USER_WITH_TOOL,
                    unifyUserIndexSet.get(UnifyUserIndex.PC_CLIENT_USER_WITH_TOOL) + 1);
            if(logTypeSet.contains("www") && ! logTypeSet.contains("fs"))
                unifyUserIndexSet.put(UnifyUserIndex.PC_CLIENT_USER_WITH_TOOL,
                    unifyUserIndexSet.get(UnifyUserIndex.PC_CLIENT_USER_WITH_TOOL) + 1);  
            
            // PC端总用户数（不含工具屏保）
            if( logTypeSet.contains(String.valueOf(LogType.PC_BOOT.ordinal())))
               unifyUserIndexSet.put(UnifyUserIndex.PC_CLIENT_USER_WITHOUT_TOOL,
                   unifyUserIndexSet.get(UnifyUserIndex.PC_CLIENT_USER_WITHOUT_TOOL) + 1);
            if(logTypeSet.contains("www") && ! logTypeSet.contains("fs"))
               unifyUserIndexSet.put(UnifyUserIndex.PC_CLIENT_USER_WITHOUT_TOOL,
                   unifyUserIndexSet.get(UnifyUserIndex.PC_CLIENT_USER_WITHOUT_TOOL) + 1);  
            
            // PC客户端启动用户数（含工具屏保）
            if( logTypeSet.contains(String.valueOf(LogType.PC_BOOT.ordinal()))
                    || logTypeSet.contains(String.valueOf(LogType.PC_AIRFIELD.ordinal()))
                    || logTypeSet.contains(String.valueOf(LogType.PC_BROWSER.ordinal()))
                    || logTypeSet.contains(String.valueOf(LogType.PC_SCREEN.ordinal())))
                       unifyUserIndexSet.put(UnifyUserIndex.PC_CLIENT_BOOT_USER_WITH_TOOL,
                           unifyUserIndexSet.get(UnifyUserIndex.PC_CLIENT_BOOT_USER_WITH_TOOL) + 1);
             // 移动端总用户数
            if( logTypeSet.contains(String.valueOf(LogType.MOBILE_BOOT.ordinal()))
                    || logTypeSet.contains(String.valueOf(LogType.MOBILE_EXIT.ordinal())))
                       unifyUserIndexSet.put(UnifyUserIndex.MOBILE_USER,
                           unifyUserIndexSet.get(UnifyUserIndex.MOBILE_USER) + 1);  
                   if( logTypeSet.contains(String.valueOf(LogType.MOBILE_PV.ordinal())))
                       unifyUserIndexSet.put(UnifyUserIndex.MOBILE_USER,
                           unifyUserIndexSet.get(UnifyUserIndex.MOBILE_USER) + 1); 
                   
           // PC客户端启动用户数
           if( logTypeSet.contains(String.valueOf(LogType.PC_BOOT.ordinal())))
               unifyUserIndexSet.put(UnifyUserIndex.PC_CLIENT_BOOT_USER,
                   unifyUserIndexSet.get(UnifyUserIndex.PC_CLIENT_BOOT_USER) + 1);
           
            // PC工具启动用户数
           if( logTypeSet.contains(String.valueOf(LogType.PC_AIRFIELD.ordinal()))
             || logTypeSet.contains(String.valueOf(LogType.PC_BROWSER.ordinal())))
                      unifyUserIndexSet.put(UnifyUserIndex.PC_TOOL_BOOT_USER,
                          unifyUserIndexSet.get(UnifyUserIndex.PC_TOOL_BOOT_USER) + 1);
           
           // PC飞机场启动用户数
           if( logTypeSet.contains(String.valueOf(LogType.PC_AIRFIELD.ordinal())))
               unifyUserIndexSet.put(UnifyUserIndex.PC_AIRFIELD_BOOT_USER,
                       unifyUserIndexSet.get(UnifyUserIndex.PC_AIRFIELD_BOOT_USER) + 1);
           
            // PC浏览器组件启动用户数
           if( logTypeSet.contains(String.valueOf(LogType.PC_BROWSER.ordinal())))
               unifyUserIndexSet.put(UnifyUserIndex.PC_BROESER_BOOT_USER,
                       unifyUserIndexSet.get(UnifyUserIndex.PC_BROESER_BOOT_USER) + 1);
           
            // PC屏保启动用户数
            if( logTypeSet.contains(String.valueOf(LogType.PC_SCREEN.ordinal())))
               unifyUserIndexSet.put(UnifyUserIndex.PC_SCREEN_BOOT_USER,
                       unifyUserIndexSet.get(UnifyUserIndex. PC_SCREEN_BOOT_USER) + 1);
            
           // PCweb用户数
            if(logTypeSet.contains("www"))
                unifyUserIndexSet.put(UnifyUserIndex.PC_WEB_USER,
                    unifyUserIndexSet.get(UnifyUserIndex.PC_WEB_USER) + 1);
            
            // 移动app用户数
            if( logTypeSet.contains(String.valueOf(LogType.MOBILE_BOOT.ordinal()))
                    || logTypeSet.contains(String.valueOf(LogType.MOBILE_EXIT.ordinal())))
                       unifyUserIndexSet.put(UnifyUserIndex.MOBILE_APP_USER,
                           unifyUserIndexSet.get(UnifyUserIndex.MOBILE_APP_USER) + 1);  
            
            // 移动web用户数   
           if( logTypeSet.contains(String.valueOf(LogType.MOBILE_PV.ordinal())))
               unifyUserIndexSet.put(UnifyUserIndex.MOBILE_WEB_USER,
                   unifyUserIndexSet.get(UnifyUserIndex.MOBILE_WEB_USER) + 1); 
	    } 
            
	    public void cleanup(Context context)
	            throws IOException, InterruptedException {
            for(Enum<UnifyUserIndex> index : unifyUserIndexSet.keySet()){                
                newKey.set(index.ordinal() + 1 + "");
                newValue.set(unifyUserIndexSet.get(index) + "");
                context.write(newKey, newValue);
	        
            }
	    }
    }
	
	public static class IdentityMapper extends
        Mapper<Text, Text, Text, Text>{
        public  void map (Text key, Text value, Context context)
           throws IOException, InterruptedException{
            context.write(key, value);
        }
   }

    public static class AddResultReducer extends
        Reducer <Text, Text, NullWritable, Text>{
        private Text newValue   = new Text();
        private String statDate = ""; 
        public void setup(Context context) throws IOException, InterruptedException{
            statDate = context.getConfiguration().get("statDate");
        }
        
        public void reduce (Text key,  Iterable <Text> values, Context context)
            throws IOException, InterruptedException{ 
            int sum = 0;
            for(Text val : values)
                sum += Integer.parseInt(val.toString());
            newValue.set(statDate + "\t" + key.toString() + "\t" + sum);
            context.write(NullWritable.get(), newValue);
        }
    }
	
	public int run(String[] args) throws Exception {
	    Configuration conf = getConf();
        GenericOptionsParser gop = new GenericOptionsParser(conf, args);
        conf = gop.getConfiguration();
        
        conf.set("statDate", conf.get("stat_date"));
        Job job = new Job(conf, "UnifyUser_" + conf.get("stat_date"));
        FileInputFormat.addInputPaths(job, conf.get("input_dir"));
        String outputDir = conf.get("output_dir");
        Path tmpOutput = new Path(outputDir + "_tmp");
        FileOutputFormat.setOutputPath(job, tmpOutput);
        tmpOutput.getFileSystem(conf).delete(tmpOutput, true);        
    
        job.setJarByClass(UnifyUserMR.class);       
        job.setMapperClass(UnifyUserMapper.class);
        job.setCombinerClass(UnifyUserCombiner.class);
        job.setReducerClass(UnifyUserReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(conf.getInt("reduce_num", 20));
        int code = job.waitForCompletion(true) ? 0 : 1;

        if(code == 0){                       
            Job combineJob = new Job(conf, "UnifyUser_" + conf.get("stat_date"));
            combineJob.setJarByClass(UnifyUserMR.class);
            
            FileInputFormat.addInputPath(combineJob, new Path(outputDir + "_tmp"));
            Path outputPath = new Path(outputDir);
            FileOutputFormat.setOutputPath(combineJob, outputPath);
            outputPath.getFileSystem(conf).delete(outputPath, true);
            
            combineJob.setMapperClass(IdentityMapper.class);
            combineJob.setReducerClass(AddResultReducer.class);            
            combineJob.setInputFormatClass(KeyValueTextInputFormat.class);
            combineJob.setOutputFormatClass(TextOutputFormat.class);
            combineJob.setMapOutputKeyClass(Text.class);
            combineJob.setOutputKeyClass(NullWritable.class);
            combineJob.setOutputValueClass(Text.class);
            
            combineJob.setNumReduceTasks(1);
            code = combineJob.waitForCompletion(true) ? 0 : 1 ;
        }
        
        FileSystem.get(conf).delete(new Path(outputDir + "_tmp"), true);
        return code;
	}

	public static void main(String[] args) throws Exception {
		int nRet = ToolRunner
				.run(new Configuration(), new UnifyUserMR(), args);
		System.out.println(nRet);
	}
}
