package com.bi.ad.intermediate.format;

import java.io.IOException;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.ad.fact.uv.AdUVutils;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.compression.lzo.LzoIndexer;

public class DeliverFormatForUVMR extends Configured implements Tool {
    
	public static class DeliverFormatForUVMapper extends
			Mapper<LongWritable, Text, Text, Text> {
	    private final static int num = 1;
	    private Text newKey   = new Text();
	    private Text newValue = new Text();
	    private HashMap<String, String> adpCode2Id = new HashMap<String, String>();
	    public void setup(Context context) throws IOException, InterruptedException{
	        adpCode2Id = AdUVutils.init("adpcode2id", 0, 1);
	    }
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String[] fields = value.toString().split("\t");
		    String fck  = fields[11];
		    String area = fields[4];
		    String requestInfo = fields[14];
		    String adpId = null;
		    String ad  = null;
		    String returnStatus = null;
		    newKey.set(fck);
		    if( ! fck.equals("-")){		        
    		    String[] arrApAdMat = requestInfo.split("\\|");	        
    	        for (int i = 0; i < arrApAdMat.length; i++) {
    	            String[] apAdMat = arrApAdMat[i].split("[:;]");
    	            try{
    	                adpId = adpCode2Id.get(apAdMat[0]).trim();
    	            }catch (Exception e){
    	                throw new  IOException(e.getMessage() + ":" + value.toString());
    	            }
    	            returnStatus = "2";  // return without ad
    	            for (int j = 1; j < apAdMat.length; j++) {
    	                String[] adMat = apAdMat[j].split("#");
    	                ad  = adMat[0];
    	                if(! ad.equals("-") && returnStatus.equals("2") ){
    	                    returnStatus = "1" ; // return with status
    	                    break;
    	                }
    	            }
    	            newValue.set(num + ":" + area + ","+ adpId + "," + returnStatus);
    	            context.write(newKey, newValue);
    	        }
	        }	       			
		}	
	}
	
    public static class DeliverFormatForUVCombiner extends
       Reducer<Text, Text, Text, Text> {
       
       private Text newValue = new Text();
       public void reduce(Text key, Iterable<Text> values, Context context)
               throws IOException, InterruptedException{      
           HashMap<String, Integer> aggResult = new HashMap<String, Integer>();
             
           for (Text val : values) {
               String[] fields = val.toString().split(":");
               int num = Integer.parseInt(fields[0]);
               String dim = fields[1];
               
               if(aggResult.containsKey(dim)){
                   aggResult.put(dim, aggResult.get(dim).intValue() + num);
               }else{
                   aggResult.put(dim, num);
               }                             
           }
           
           for (String hashKey : aggResult.keySet()) {
               newValue.set(aggResult.get(hashKey).toString() + ":" + hashKey);
               context.write(key, newValue);
           }
       }
    }
    
	public static class DeliverFormatForUVReducer extends
			Reducer<Text, Text, Text, Text> {
	   private Text newValue = new Text();
	   
	   public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{			
	       HashMap<String, Integer> aggResult = new HashMap<String, Integer>();

	       newValue.clear();
		   for (Text val: values) {
               String[] fields = val.toString().split(":");
               int num = Integer.parseInt(fields[0]);
               String dim = fields[1];
               
               if(aggResult.containsKey(dim)){
                   aggResult.put(dim, aggResult.get(dim).intValue() + num);
               }else{
                   aggResult.put(dim, num);
               }    
		   }
			
		  for (String hashKey : aggResult.keySet()) {
			  String singleValue =  hashKey + "," + aggResult.get(hashKey).toString();
			  String tab = "\t";
			  if(newValue.getLength() > 0)
			      newValue.append(tab.getBytes(), 0, tab.length());
			  newValue.append(singleValue.getBytes(), 0, singleValue.length());
		  }
		  
		  context.write(key, newValue);
		}	
	}
	
	public static class IdentityMapper extends
	    Mapper<Text, Text, Text, Text>{
	    public  void map (Text key, Text value, Context context)
	            throws IOException, InterruptedException{
	        context.write(key, value);
	    }
	}

	public static class IdentityReducer extends
	    Reducer <Text, Text, Text, Text> {
	    public  void reduce (Text key,  Iterable <Text> values, Context context)
           throws IOException, InterruptedException{
	        for(Text val : values)
	            context.write(key, val);
	    }
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
		conf = optionparser.getConfiguration();

		Job job = new Job(conf, conf.get("job_name"));
		job.setJarByClass(DeliverFormatForUVMR.class);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
	    String outputDir = conf.get("output_dir");
	    String tmpDir    = outputDir + "_tmp";
		Path tmpOut      = new Path(tmpDir);	    
		FileOutputFormat.setOutputPath(job, tmpOut);
		tmpOut.getFileSystem(conf).delete(tmpOut, true);

		job.setMapperClass(DeliverFormatForUVMapper.class);
		job.setCombinerClass(DeliverFormatForUVCombiner.class);
		job.setReducerClass(DeliverFormatForUVReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(conf.getInt("reduce_num", 20));

		int code = job.waitForCompletion(true) ? 0 : 1;
		
		if(code == 0){
		    
		    // this job is for combining  small files into one
		    Job combineJob = new Job(conf, "CombineTmpData");
		    combineJob.setJarByClass(DeliverFormatForUVMR.class);
		    
		    FileInputFormat.addInputPath(combineJob,  new Path(tmpDir));
		    FileOutputFormat.setOutputPath(combineJob, new Path(outputDir));
		    combineJob.setMapperClass(IdentityMapper.class);
		    combineJob.setReducerClass(IdentityReducer.class);
		    
		    combineJob.setInputFormatClass(KeyValueTextInputFormat.class);
		    combineJob.setOutputFormatClass(TextOutputFormat.class);
		    
		    combineJob.setOutputKeyClass(Text.class);
		    combineJob.setOutputValueClass(Text.class);
		    
		    TextOutputFormat.setCompressOutput(combineJob, true);
		    TextOutputFormat.setOutputCompressorClass(combineJob, LzopCodec.class);
		    
		    combineJob.setNumReduceTasks(1);
		    code = combineJob.waitForCompletion(true) ? 0 : 1 ;
		}
		
		FileSystem.get(conf).delete(tmpOut, true);	
		LzoIndexer lzoIndexer = new LzoIndexer(conf);
		lzoIndexer.index(new Path(outputDir));
		System.exit(code);
		return code;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new DeliverFormatForUVMR(), args);
		System.out.println(res);
	}
}
