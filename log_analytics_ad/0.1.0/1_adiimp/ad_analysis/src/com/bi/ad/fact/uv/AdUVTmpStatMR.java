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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class AdUVTmpStatMR extends Configured implements Tool {

	public static class AdUVTmpStatMapper extends
			Mapper<LongWritable, Text, Text, Text> {
	    
	    private ArrayList<String> adpList  = new ArrayList<String>();
	    private ArrayList<String> areaList = new ArrayList<String>();  	    
	    private Text newValue = new Text();
	    private Text newKey   = new Text();
	    private String date   = null;
	    private String sep    = "\t";
	    public void setup(Context context) throws IOException, InterruptedException{
            try {
                adpList =  AdUVutils.init("adp_list", 0);
                areaList = AdUVutils.init("area_list", 0);
                String[] pathNames = ((FileSplit) context.getInputSplit()).getPath()
                                    .getParent().toUri().getRawPath().split("/");
                date = pathNames[pathNames.length - 3] + pathNames[pathNames.length - 2] + pathNames[pathNames.length - 1];
            } catch ( IOException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
	    
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {		    
		    String[] fields = value.toString().split("\t");
		    String fck  = fields[0]; 
		    String area = null;
		    String adp  = null;
		    String tmp  = null;
		    newKey.set(fck);
		    newValue.clear();
		    for (int i = 1; i < fields.length; i++) {
	            String[] detail = fields[i].split(",");
	            area = detail[0];
	            adp  = detail[1];
	            tmp  = fields[i] + "," + date;
	            
	            if (! adpList.contains(adp) || ! AdUVutils.legalArea(areaList, area))
	                continue; 
	               
                if(newValue.getLength() > 0 )
                   newValue.append(sep.getBytes(), 0, sep.length());
                newValue.append(tmp.getBytes(), 0, tmp.length());
	        }	
		    if(newValue.getLength() > 0)
		        context.write(newKey, newValue);
		}	
	}
	   
	public static class AdUVTmpStatReducer extends
			Reducer<Text, Text, NullWritable, Text> {
      
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
          context.write(NullWritable.get(), newValue);        
	    }
	}	
	
	public static class UVResultMapper extends
	    Mapper<LongWritable, Text, Text, Text>{
       private Text newValue = new Text();
       private Text newKey   = new Text();
       private HashMap<String, String> adpMap    = new HashMap<String, String>();
       private HashMap<String, String> areaMap   = new HashMap<String, String>();
       private HashMap<String, String> statusMap = new HashMap<String, String>();
       private HashMap<String, Integer> nuvMap   = new HashMap<String, Integer>();
       private HashMap<String, Set<String>> duvMap = new HashMap<String, Set<String>>();

       public void setup(Context context) throws IOException, InterruptedException{
           try {
               adpMap =  AdUVutils.init("adp_list", 0, 1);
               areaMap =  AdUVutils.init("area_list",0, 1);
               adpMap.put("0", "汇总");
               areaMap.put("0", "汇总");  
               statusMap.put("0", "请求");
               statusMap.put("1", "返回");
               statusMap.put("2", "未返回");
           } catch ( IOException e) {
               e.printStackTrace();
               System.exit(0);
           }
       }  
	    
	    public  void map (LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
	       nuvMap.clear();
	       duvMap.clear();
	       String dim = null;         
           String[] fields = value.toString().split("\t");
           for(int i = 0; i < fields.length; i++){
               String[] dims = fields[i].split(",");
               String area = dims[0];
               String adp  = dims[1];
               String status = dims[2];
               int    num   = Integer.parseInt(dims[3]);
               String[]  dateArr = dims[4].split(":");
               
               ArrayList<String> desArea= AdUVutils.getAreaList(areaMap, area);
               for (String areaTmp : desArea){
                   for (String adpTmp : new String[]{"0", adp}){
                       for (String statTmp : new String[]{"0", status}){
                           dim = areaTmp + "," + adpTmp + "," + statTmp;
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
               String[] dims = dimKey.split(",");
               String areaName = areaMap.get(dims[0]);
               String adpName  = adpMap.get(dims[1]);
               String statusName = statusMap.get(dims[2]);
               int nuv = nuvMap.get(dimKey);
               int duv = duvMap.get(dimKey).size();
               newKey.set(adpName + "," + statusName + "," + areaName);
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
        
    public static class UVResultReducer extends
        Reducer <Text, Text, NullWritable, Text> {
        private Text newValue = new Text();
        private UVFreq nuv = new UVFreq(20);
        private UVFreq duv = new UVFreq(20);
        public void setup(Context context) throws IOException, InterruptedException{
            String title = "广告位名称,请求返回状态,地域名称," 
                         + "NUV1,NUV2,NUV3,NUV4,NUV5,NUV6,NUV7,NUV8,NUV9,NUV10,"
                         + "NUV11,NUV12,NUV13,NUV14,NUV15,NUV16,NUV17,NUV18,NUV19,NUV20+,"
                         + "DUV1,DUV2,DUV3,DUV4,DUV5,DUV6,DUV7,DUV8,DUV9,DUV10,"
                         + "DUV11,DUV12,DUV13,DUV14,DUV15,DUV16,DUV17,DUV18,DUV19,DUV20+";
            newValue.set(title);
            context.write(NullWritable.get(), newValue);
        }
        public  void reduce (Text key,  Iterable <Text> values, Context context)
            throws IOException, InterruptedException{
            nuv.clear();
            duv.clear();
            for(Text val : values){
                String[] uvNum = val.toString().split(":");                
                String[] nuvPair = uvNum[0].split(",");
                String[] duvPair = uvNum[1].split(",");
                for(int i = 0; i < nuvPair.length; i += 2 ){                 
                    nuv.addFreq(Integer.parseInt(nuvPair[i]), Integer.parseInt(nuvPair[i + 1])); 
                }
                for(int i = 0; i < duvPair.length; i += 2 ){                 
                    duv.addFreq(Integer.parseInt(duvPair[i]), Integer.parseInt(duvPair[i + 1])); 
                }
            }
            newValue.set(key + "," + nuv.toString(",") + "," + duv.toString(","));
            context.write(NullWritable.get(), newValue);
        }
    }

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
		conf = optionparser.getConfiguration();

		Job job = new Job(conf, "TmpUVStatCombine");
		job.setJarByClass(AdUVTmpStatMR.class);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
	    String outputDir = conf.get("output_dir");
	    String tmpDir    = outputDir + "_tmp";
	    Path tmpOut      = new Path(tmpDir);        
	    FileOutputFormat.setOutputPath(job, tmpOut);
	    tmpOut.getFileSystem(conf).delete(tmpOut, true);

		job.setMapperClass(AdUVTmpStatMapper.class);
		job.setReducerClass(AdUVTmpStatReducer.class);

		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(conf.getInt("reduce_num", 20));

        int code = job.waitForCompletion(true) ? 0 : 1;
        
        if(code == 0){                       
            Job combineJob = new Job(conf, "TmpUVStatResult");
            combineJob.setJarByClass(AdUVTmpStatMR.class);
            
            FileInputFormat.addInputPath(combineJob,  new Path(tmpDir));
            FileOutputFormat.setOutputPath(combineJob, new Path(outputDir));
            
            combineJob.setMapperClass(UVResultMapper.class);
            combineJob.setCombinerClass(AdUVStatCombiner.class);
            combineJob.setReducerClass(UVResultReducer.class);
            
            combineJob.setInputFormatClass(TextInputFormat.class);
            combineJob.setOutputFormatClass(TextOutputFormat.class);
            
            
            combineJob.setMapOutputKeyClass(Text.class);
            combineJob.setOutputKeyClass(NullWritable.class);
            combineJob.setOutputValueClass(Text.class);
            
            combineJob.setNumReduceTasks(1);
            code = combineJob.waitForCompletion(true) ? 0 : 1 ;
        }
        
        FileSystem.get(conf).delete(tmpOut, true);  
        System.exit(code);
        return code;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new AdUVTmpStatMR(), args);
		System.out.println(res);
	}
}
