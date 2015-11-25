package com.bi.ad.fact.uv;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.bi.ad.comm.util.format.AreaMappingInfo;

public class AdUVStatUniversalMR extends Configured implements Tool {

	public static class AdUVCombineUserDataMapper extends
			Mapper<LongWritable, Text, Text, Text> {
	    
	    private String   date = null;
	    private Text newValue = new Text();
	    private Text   newKey = new Text();	 
 	    public void setup(Context context) throws IOException, InterruptedException{
	        String[] pathNames = ((FileSplit) context.getInputSplit()).getPath()
                                    .getParent().toUri().getRawPath().split("/");
	        // this date parameter is meaningful only when the input data belongs to a single date.
            date = pathNames[pathNames.length - 3] + pathNames[pathNames.length - 2] + pathNames[pathNames.length - 1];     
        }
	    
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringBuilder strValue = new StringBuilder(1000);
		    String[] fields = value.toString().split("\t");
		    String fck  = fields[0]; 
		    newKey.set(fck);
	        for (int i = 1; i < fields.length; i++){
	            String[] detail = fields[i].split(",");	           
	            if(detail.length == 5)   // record collection of a period
	                break;
	            if(detail.length == 4){  // record of a single day
	                if(strValue.length() > 0)
	                    strValue.append("\t");
	                strValue.append(fields[i]).append(",").append(date); 
	            }	            
	        }
	        
	        if(strValue.length() > 0){
	            newValue.set(strValue.toString());
	        }else{
	            newValue.set(value.toString().substring((fck + "\t").length()));
	        }	        
	        context.write(newKey, newValue);
		}	
	}
	    
	public static class  AdUVCombineUserDataReducer extends
			Reducer<Text, Text, Text, Text> {
	    
       private Text newValue = new Text();
       private String sep = "\t";
       private HashMap<String, Integer> nuvMap = new HashMap<String, Integer>();
       private HashMap<String, String>  duvMap = new HashMap<String, String>();

	   public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{	       	       
	       nuvMap.clear();
	       duvMap.clear();
	       newValue.clear();
	       String dim = null;
	       String tmp = null;
		   for (Text value : values) {
		       for(String val : value.toString().split("\t")){
                   String[] fields = val.split(",");              
                   String area   = fields[0];
                   String adp    = fields[1];
                   String status = fields[2];
                   int    num    = Integer.parseInt(fields[3]);
                   String date   = fields[4];                   
                   dim = area + "," + adp + "," + status;
                   if (nuvMap.containsKey(dim))                              
                       nuvMap.put(dim,  nuvMap.get(dim) + num);
                   else
                       nuvMap.put(dim, num);
                                
                   if(duvMap.containsKey(dim))
                       duvMap.put(dim, duvMap.get(dim) + ":" + date);
                   else
                       duvMap.put(dim, date);
		       }
		   }
		   for (String dimKey : nuvMap.keySet()){
		       tmp = dimKey + "," + nuvMap.get(dimKey) + "," + duvMap.get(dimKey);
		       if(newValue.getLength() > 0 )
		           newValue.append(sep.getBytes(), 0, sep.length());
		       newValue.append(tmp.getBytes(), 0, tmp.length());
		   }

		   context.write(key, newValue);   
	   }
	}
	
	public static class AdUVStatMapper extends
	    Mapper<LongWritable, Text, Text, Text>{	  

	    private String statDate = null;
	    private Text newValue   = new Text();
	    private Text newKey     = new Text();
	    private AreaMappingInfo areaMapping = new AreaMappingInfo();
	    private ArrayList<String> areaList  = new ArrayList<String>(4);
	    private ArrayList<String> adpList   = new ArrayList<String>(4);
	    private HashMap<String, Integer> nuvMap = new HashMap<String, Integer>();
        private HashMap<String, Set<String>> duvMap = new HashMap<String, Set<String>>();
	    private HashMap<String, ArrayList<String>> adpGroup = new HashMap<String, ArrayList<String>>();

	    public void setup(Context context) throws IOException, InterruptedException{
	        adpGroup   = AdUVutils.init("adp_group");
	        areaMapping.init("area_mapping_info");	
	        statDate = context.getConfiguration().get("statDate");
	    }
	    
	    public void map (LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
	        nuvMap.clear();
	        duvMap.clear();
	        String dim = null;	        
	        String[] fields = value.toString().split("\t");
	        for(int i = 1; i < fields.length; i++){
	             String[] dims = fields[i].split(",");
	             String area = dims[0];
	             String adp  = dims[1];
	             String status = dims[2];
	             int    num   = Integer.parseInt(dims[3]);
	             String[]  dateArr = dims[4].split(":");
	             
	             areaList = AdUVutils.getAreaList(areaMapping, area, statDate);
	             adpList  = AdUVutils.getAdpList(adpGroup, adp);
	             String[] statusList = {"0", status};
	             
	             for(String tmpArea : areaList){
	                 for(String tmpAdp : adpList){
	                     for(String tmpStatus : statusList){
	                         dim = tmpArea + "," + tmpAdp + "," + tmpStatus;
	                         if (nuvMap.containsKey(dim)){                              
	                             nuvMap.put(dim,  nuvMap.get(dim) + num);
	                         }else{
	                             nuvMap.put(dim, num);
	                         }
	                         
	                         if(! duvMap.containsKey(dim)){                                  
	                             Set<String> set = new HashSet<String>();
	                             duvMap.put(dim, set);
	                         }
	                         duvMap.get(dim).addAll(Arrays.asList(dateArr));                          
	                     }
	                 }
	             }	             
	         }
	         
	        for (String dimKey : nuvMap.keySet()){
	            int nuv = nuvMap.get(dimKey);
	            int duv = duvMap.get(dimKey).size();	            
	            newKey.set(dimKey);
	            newValue.set(nuv + ",1:" + duv + ",1");
	            context.write(newKey, newValue);
	        }
        }
	}	
    
    public static class AdUVStatCombiner extends
        Reducer <Text, Text, Text, Text> {

        private int    uvNum  = 0;
        private String uvKey  = null;
        private Text newValue = new Text(); 
        private HashMap<String, Integer> nuvMap = new HashMap<String, Integer>();
        private HashMap<String, Integer> duvMap = new HashMap<String, Integer>();
        public  void reduce (Text key, Iterable <Text> values, Context context)
            throws IOException, InterruptedException{
            StringBuilder str = new StringBuilder(500);
            nuvMap.clear();
            duvMap.clear();
            for(Text val : values){
                String[] uvSet = val.toString().split(":");
                String[] nuvPair = uvSet[0].split(",");
                String[] duvPair = uvSet[1].split(",");
                for(int i = 0; i < nuvPair.length; i += 2 ){
                    uvKey = nuvPair[i];
                    uvNum = Integer.parseInt(nuvPair[i+1]);
                    if (nuvMap.containsKey(uvKey)){                              
                        nuvMap.put(uvKey, nuvMap.get(uvKey) + uvNum);
                    }else{
                        nuvMap.put(uvKey, uvNum);
                    }                      
                }
                
                for(int i = 0; i < duvPair.length; i += 2 ){
                    uvKey = duvPair[i];
                    uvNum = Integer.parseInt(duvPair[i+1]);
                    if (duvMap.containsKey(uvKey)){                              
                        duvMap.put(uvKey, duvMap.get(uvKey) + uvNum);
                    }else{
                        duvMap.put(uvKey, uvNum);
                    }                      
                }
            }
            
            for(String nuv : nuvMap.keySet()){
                if(str.length() > 0)
                    str.append(",");
                str.append(nuv).append(",").append(nuvMap.get(nuv));               
            }
            
            int flag = 1;
            for(String duv : duvMap.keySet()){
                if(flag == 1 ){
                    str.append(":");
                    flag = 0;
                }else{
                    str.append(",");
                }
                str.append(duv).append(",").append(duvMap.get(duv));               
            }
            
            newValue.set(str.toString());
            context.write(key, newValue);
       }
    }
    
    public static class AdUVStatReducer extends
        Reducer <Text, Text, NullWritable, Text> {
        
        private int maxNuv = 0;
        private int maxDuv = 0; 
        private UVFreq nuvFreq = null;
        private UVFreq duvFreq = null;
        private String statDate = null;      
        private String period   = null;
        private Text newValue   = new Text();
        private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
        
        public void setup(Context context) throws IOException, InterruptedException{
            statDate = context.getConfiguration().get("statDate");
            period = context.getConfiguration().get("period");  
            maxNuv = context.getConfiguration().getInt("maxNuv", 10);
            maxDuv = context.getConfiguration().getInt("maxDuv", 1);
            nuvFreq = new UVFreq(maxNuv);
            duvFreq = new UVFreq(maxDuv);
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }
        
        public void reduce (Text key,  Iterable <Text> values, Context context)
            throws IOException, InterruptedException{
            nuvFreq.clear();
            duvFreq.clear();            
            for(Text val : values){
                String[] uvNum = val.toString().split(":");                
                String[] nuvPair = uvNum[0].split(",");
                String[] duvPair = uvNum[1].split(",");
                for(int i = 0; i < nuvPair.length; i += 2 ){                 
                    nuvFreq.addFreq(Integer.parseInt(nuvPair[i]), Integer.parseInt(nuvPair[i + 1])); 
                }
                for(int i = 0; i < duvPair.length; i += 2 ){                 
                    duvFreq.addFreq(Integer.parseInt(duvPair[i]), Integer.parseInt(duvPair[i + 1])); 
                }
            }
            
            String area = key.toString().split(",", 1)[0];
            String dimKey = statDate + "\t" + key.toString().replaceAll(",", "\t").replaceFirst("^[a-z]", "");            
            String tableMiddleName = AdUVutils.getTableMiddleName(area);
            
            // output DUV table
            if(maxDuv != 1 ){
                newValue.set(dimKey + "\t" + duvFreq.toString("\t"));
                multipleOutputs.write(NullWritable.get(), newValue, "F_AD_" + period + "_" + tableMiddleName + "_DUV");
            }
                    
            // output NUV table                
            newValue.set(dimKey + "\t" + nuvFreq.toString("\t"));
            multipleOutputs.write(NullWritable.get(), newValue, "F_AD_" + period + "_"  + tableMiddleName + "_NUV");            
        }
        
        public void cleanup(Context context) throws IOException,InterruptedException {
            multipleOutputs.close();
        }
    }

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
		conf = optionparser.getConfiguration();
		
		conf.set("statDate", conf.get("stat_date"));
		conf.set("maxNuv", conf.get("max_nuv"));
		conf.set("maxDuv", conf.get("max_duv"));
		conf.set("period", conf.get("period"));
		
		Job job = new Job(conf, "UVStatCombineUserData");
		job.setJarByClass(AdUVStatUniversalMR.class);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
	    String outputDir = conf.get("output_dir");
	    String tmpDir    = outputDir + "_tmp";
	    Path tmpOut      = new Path(tmpDir);        
	    FileOutputFormat.setOutputPath(job, tmpOut);
	    tmpOut.getFileSystem(conf).delete(tmpOut, true);

		job.setMapperClass(AdUVCombineUserDataMapper.class);
		job.setReducerClass(AdUVCombineUserDataReducer.class);

		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	    TextOutputFormat.setCompressOutput(job, true);
	    TextOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
        
		job.setNumReduceTasks(conf.getInt("reduce_num", 20));
		
        int code = job.waitForCompletion(true) ? 0 : 1;
        
        //compress tmp output directory        
        LzoIndexer lzoIndexer = new LzoIndexer(conf);
        lzoIndexer.index(new Path(tmpDir));
        if(code == 0){                       
            Job combineJob = new Job(conf, "UVStatResult");
            combineJob.setJarByClass(AdUVStatUniversalMR.class);
            
            FileInputFormat.addInputPath(combineJob,  new Path(tmpDir));
            Path outputPath =   new Path(outputDir);
            FileOutputFormat.setOutputPath(combineJob, outputPath);
            outputPath.getFileSystem(conf).delete(outputPath, true);
            
            combineJob.setMapperClass(AdUVStatMapper.class);
            combineJob.setCombinerClass(AdUVStatCombiner.class);
            combineJob.setReducerClass(AdUVStatReducer.class);
            
            combineJob.setInputFormatClass(LzoTextInputFormat.class);
            combineJob.setOutputFormatClass(TextOutputFormat.class);
            combineJob.setMapOutputKeyClass(Text.class);
            combineJob.setOutputKeyClass(NullWritable.class);
            combineJob.setOutputValueClass(Text.class);
            
            combineJob.setNumReduceTasks(1);
            code = combineJob.waitForCompletion(true) ? 0 : 1 ;
        }
        
        
        if(conf.get("period").equals("DATE")){
            FileSystem.get(conf).delete(tmpOut, true);
        }
        System.exit(code);
        return code;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new AdUVStatUniversalMR(), args);
		System.out.println(res);
	}
}
