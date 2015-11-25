package com.bi.ad.fact.uv;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.bi.ad.comm.util.format.AreaMappingInfo;

public class AdUVStatDateMR extends Configured implements Tool {
	
	public static class AdUVStatMapper extends
	    Mapper<LongWritable, Text, Text, Text>{

	    private String statDate = null;
	    private Text     newKey = new Text();
	    private Text   newValue = new Text();
	    private AreaMappingInfo areaMapping = new AreaMappingInfo();
	    private HashMap<String, Integer> nuvMap = new HashMap<String, Integer>();
	    private ArrayList<String> areaList = new ArrayList<String>(4);
	    private ArrayList<String> adpList  = new ArrayList<String>(4);
	    private HashMap<String, ArrayList<String>> adpGroup  = new HashMap<String, ArrayList<String>>();	    
	    
	    public void setup(Context context) throws IOException, InterruptedException{
	        adpGroup   = AdUVutils.init("adp_group");
	        areaMapping.init("area_mapping_info");	
	        statDate = context.getConfiguration().get("statDate");
	    }
	    
	    public void map (LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
	        nuvMap.clear();      	               
	        String[] fields = value.toString().split("\t");
	        String dim = null;
	        for(int i = 1; i < fields.length; i++){
	             String[] dims = fields[i].split(",");
	             String area   = dims[0];
	             String adp    = dims[1];
	             String status = dims[2];
	             int    num    = Integer.parseInt(dims[3]);	             
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
	                     }
	                 }
	             }	             
	         }
	         
	        for (String dimKey : nuvMap.keySet()){		        
	            newValue.set(nuvMap.get(dimKey)  + ",1");
	            newKey.set(dimKey);
	            context.write(newKey, newValue);
	        }
        }
	}	
    
    public static class AdUVStatCombiner extends
        Reducer <Text, Text, Text, Text> {
        
        private int    uvNum  = 0;
        private String uvKey  = null;
        private Text newValue = new Text(); 
        private HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
        
        public  void reduce (Text key, Iterable <Text> values, Context context)
            throws IOException, InterruptedException{
            StringBuilder str = new StringBuilder(100);
            hashMap.clear();
            for(Text val : values){
                String[] uvPair = val.toString().split(",");
                for(int i = 0; i < uvPair.length; i += 2 ){
                    uvKey = uvPair[i];
                    uvNum = Integer.parseInt(uvPair[i+1]);
                    if (hashMap.containsKey(uvKey)){                              
                        hashMap.put(uvKey, hashMap.get(uvKey) +  uvNum);
                    }else{
                        hashMap.put(uvKey, uvNum);
                    }                      
                }
            }
            
            for(String uvn : hashMap.keySet()){
                if(str.length() > 0)
                    str.append(",");
                str.append(uvn).append(",").append(hashMap.get(uvn));      
            }
            
            newValue.set(str.toString());
            context.write(key, newValue);
       }       
    }
    
    public static class AdUVStatReducer extends
        Reducer <Text, Text, NullWritable, Text> {
        
        private int      maxNuv = 0; 
        private UVFreq   uvFreq = null;  
        private String statDate = null;      
        private String   period = "DATE";
        private Text   newValue = new Text();  
        private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
        
        public void setup(Context context) throws IOException, InterruptedException{           
            maxNuv = context.getConfiguration().getInt("maxNuv", 10);
            uvFreq = new UVFreq(maxNuv);
            statDate = context.getConfiguration().get("statDate");  
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);   
        }
        
        public void reduce (Text key, Iterable <Text> values, Context context)
            throws IOException, InterruptedException{
            uvFreq.clear();
            for(Text val : values){
                String[] uvPair = val.toString().split(",");
                for(int i = 0; i < uvPair.length; i += 2 ){                 
                    uvFreq.addFreq(Integer.parseInt(uvPair[i]), Integer.parseInt(uvPair[i + 1])); 
                }
            }
            
            String area = key.toString().split(",", 1)[0];
            String dimKey = statDate + "\t" + key.toString().replaceAll(",", "\t").replaceFirst("^[a-z]", "");            
            String tableMiddleName = AdUVutils.getTableMiddleName(area);
                    
            // output NUV table                
            newValue.set(dimKey + "\t" + uvFreq.toString("\t"));
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
		Job job = new Job(conf, "ADUVStatDate");
		job.setJarByClass(AdUVStatDateMR.class);
		
		FileInputFormat.addInputPath(job, new Path(conf.get("input_dir")));
		Path outputPath = new Path(conf.get("output_dir"));
		FileOutputFormat.setOutputPath(job, outputPath );
        outputPath.getFileSystem(conf).delete(outputPath, true);
        
        job.setMapperClass(AdUVStatMapper.class);
        job.setCombinerClass(AdUVStatCombiner.class);
        job.setReducerClass(AdUVStatReducer.class);
        
        job.setInputFormatClass(LzoTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(1);
        int code = job.waitForCompletion(true) ? 0 : 1 ;       
        System.exit(code);
        return code;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new AdUVStatDateMR(), args);
		System.out.println(res);
	}
}
