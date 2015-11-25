
package com.bi.ad.fact.adiimp;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.bi.ad.comm.util.format.*;
import com.bi.ad.logs.click.format.*;
import com.bi.ad.logs.deliver.format.*;
import com.bi.ad.logs.monitor.format.*;

public class ADIIMPMR extends Configured implements Tool {

	public static class ADIIMPMRMapper extends
			Mapper<LongWritable, Text, Text, Text> {
	   private static String date = null;
	   private AdpInfo adpInfo = new AdpInfo();
       private AreaMappingInfo areaMapping = new AreaMappingInfo();

       public void setup(Context context) throws IOException, InterruptedException{
            try {   
                adpInfo.init("adp_info"); 
                areaMapping.init("area_mapping_info");
                Configuration conf = context.getConfiguration(); 
                date  = conf.get("stat_date");   
            } catch ( IOException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    ADIIndexContainer indexContainer = new ADIIndexContainer();
		    StringBuilder keyStr = new StringBuilder();
		    String        sep    = "\t";
			String[] fields    = value.toString().split("\t");
			String logType     = fields[0];
			String cityId      = fields[4];
			String channelId   = fields[5];
			String copyrightId = fields[7];
			String adpCode     = null;
			String adpId       = null;
	        String agentAreaId = areaMapping.getAgentAreaId(cityId, date);
			
			if(DeliverLog.isDeliverLog(logType)){
			    int     num = 1;
			    String adpm = fields[14];
			    String[] adpArr = adpm.split("\\|");
			    for(int i = 0 ; i < adpArr.length; i++){
			        StringBuilder tmpStr = new StringBuilder();
			        String[] detail = adpArr[i].split("[:;]");
			        adpCode = detail[0];
			        adpId   = adpInfo.getAdpInfo().get(adpCode).getId();
			        if(adpInfo.getAdpInfo().get(adpCode).getTypeId().equals("5")) // 5 代表文字链类
			            num = detail.length  - 1; 
			        if(channelId.equals("-")){
			            if(adpInfo.getAdpInfo().get(adpCode).getOptFlag().equals("1")){
			                channelId = "81"; //optimized
			            }else{
			                channelId = "82"; // unknown
			            }			                
			        }
			        indexContainer.setBrowse(num);
			        tmpStr.append(date).append(sep).append(agentAreaId).append(sep).append(channelId)
			              .append(sep).append(adpId).append(sep).append(copyrightId);			        
			        context.write(new Text(tmpStr.toString()), new Text(indexContainer.toString())); 
			    }
			}
			
			if(MonitorLog.isMonitorLog(logType)){
			    adpCode = fields[14];
			    adpId   = adpInfo.getAdpInfo().get(adpCode).getId();
			    int playTime = Integer.parseInt(fields[17]);			    
			    int playDur  = Integer.parseInt(fields[18]);
			    if(playDur == 0 )
			        playDur = 15000;
			  
			    if(playTime == 0)
			        indexContainer.setPlay(1);
			    if(playTime > 5000)
			        indexContainer.setEffePlay(1);
			    if(playTime > (playDur - 1000))
			        indexContainer.setFullPlay(1);	
			    keyStr.append(date).append(sep).append(agentAreaId).append(sep).append(channelId)
			          .append(sep).append(adpId).append(sep).append(copyrightId);
			    if (!indexContainer.isEmpty())
			        context.write(new Text(keyStr.toString()), new Text(indexContainer.toString()));
			}
			
			if(ClickLog.isClickLog(logType)){
	            adpCode = fields[14];
	            adpId   = adpInfo.getAdpInfo().get(adpCode).getId();
			    indexContainer.setClick(1);
			    keyStr.append(date).append(sep).append(agentAreaId).append(sep).append(channelId)
			          .append(sep).append(adpId).append(sep).append(copyrightId);
			    context.write(new Text(keyStr.toString()), new Text(indexContainer.toString()));
			}
		}
	}

	public static class ADIIMPMRReducer extends
			Reducer<Text, Text, Text, Text> {	    
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {			
			ADIIndexContainer indexContainer = new ADIIndexContainer();
            for (Text ic : values) 
                    indexContainer.add(ic.toString());
            context.write(key, new Text(indexContainer.toString()));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();
		
		Job job = new Job(conf, conf.get("job_name"));
		job.setJarByClass(ADIIMPMR.class);
	    String statDate= conf.get("stat_date");        
	    conf.set("stat_date",statDate);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("output_dir")));
	   
		job.setMapperClass(ADIIMPMRMapper.class);
		job.setCombinerClass(ADIIMPMRReducer.class);
		job.setReducerClass(ADIIMPMRReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(conf.getInt("reduce_num", 20));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ADIIMPMR(), args);
		System.out.println(res);
	}
 }
