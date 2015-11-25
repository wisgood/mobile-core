package com.bi.client.uninstall.format;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.bi.client.util.IpParser1;
import com.bi.client.util.MACFormat1;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class UninstallLogFormatMR extends Configured implements Tool {
	
	public static class ClientUninstallFormatMapper extends
			Mapper<LongWritable, Text, NullWritable, Text>{
		private static final int OTHERSOFTWARE_NUM = 11;
		private static String date = null;
		private IpParser1 ipParser = null;
		private static Logger logger = Logger.getLogger(UninstallLogFormatMR.class.getName());
		private static final String RECORDID_DEFAULT = "-";
		private static final String MAC_DEFAULT = "-";
		private static final String CHANNELID_DEFAULT = "1";
		private static final String RUNCOUNT_DEFAULT = "0";
		private static final String MODIFYHISTORY_DEFAULT = "0";
		private static final String TASKCOUNT_DEFAULT = "-";
		private static final String LASTCRASH_DEFAULT = "-";
		private static final String MAXDOWNLOAD_DEFAULT = "2";
		private static final String TOTALHIGH_DEFAULT = "-";
		private static final String TOTALLOW_DEFAULT = "-";
		
		public void setup(Context context) throws IOException, InterruptedException{
			ipParser = new IpParser1();
			try{
				ipParser.init("ip_table");
				Configuration conf = context.getConfiguration();
				date = conf.get("stat_date");
			}catch(IOException e){
				e.printStackTrace();
				System.exit(0);
			}	
		}
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String originLog = value.toString();
			HashMap<String, String> indexHashMap = new HashMap<String, String>();
			String recordId = RECORDID_DEFAULT;
			String ipOrigin = null;
			String timestamp = null;			
			String versionOrigin = null;
			String macOrigin = MAC_DEFAULT;			
			String channelId = CHANNELID_DEFAULT;
			String runCount = RUNCOUNT_DEFAULT;
			StringBuilder otherSoftwareSB = new StringBuilder();
			String modifyHistory = MODIFYHISTORY_DEFAULT;
			String taskCount = TASKCOUNT_DEFAULT ;
			String lastCrash = LASTCRASH_DEFAULT;
			String maxDownload = MAXDOWNLOAD_DEFAULT;
			String totalHigh = TOTALHIGH_DEFAULT;
			String totalLow = TOTALLOW_DEFAULT; 
			
			try {
				String[] fields = originLog.split("&");                 //ip=&u=&time=&s=
				for(int i=0; i < fields.length; i++){
					String patternStr = "^(.+)=(.+)$";
					if(Pattern.matches(patternStr, fields[i])){
						String[] index = fields[i].split("=");
						indexHashMap.put(index[0],index[1]);
					}
				}
				
				//record_id
				if(indexHashMap.containsKey("record_id")){
					recordId = indexHashMap.get("record_id");
				}
		
				//longIp,city_id,isp_id
				if(indexHashMap.containsKey("ip")){
					ipOrigin = indexHashMap.get("ip");
				}
				long longIp = 0;
				longIp = IpParser1.ip2long(ipOrigin);
				String provinceId = ipParser.getAreaId(longIp);
				String ispId = ipParser.getIspId(longIp);
				
				//date_id
				if(indexHashMap.containsKey("time")){
					timestamp = indexHashMap.get("time");					
				}
//				String timeStampStr = TimeFormat.toString(timestamp);
//				String dateId = timeStampStr.split("\t")[1];
				
				//mac
				if(indexHashMap.containsKey("s")){
					macOrigin = indexHashMap.get("s");
				}
				String macFormat = MACFormat1.macFormatToCorrectStr(MACFormat1.macFormat(macOrigin));
				
				//versionId
				if(indexHashMap.containsKey("v")){
					versionOrigin = indexHashMap.get("v");
				}
				long versionId = 0l;
				versionId = IpParser1.ip2long(versionOrigin);

				//channelId
				if(indexHashMap.containsKey("i")){
					channelId = indexHashMap.get("i");
					if(!Pattern.matches("^\\d{1,6}$", channelId)){
					channelId = CHANNELID_DEFAULT;
					}
				}

				//runCount
				if(indexHashMap.containsKey("r")){
					runCount= indexHashMap.get("r");
				}
				
//				otherSoftwareSB.append("-");
				
				//otherSoftware
				if(indexHashMap.containsKey("other")){
					String other = indexHashMap.get("other");
					for(int j = 0; j < OTHERSOFTWARE_NUM; j++){
						char flag = '0';
						if(j > 0){
							otherSoftwareSB.append("\t");
						}
						if(j < other.length()){
							flag = other.charAt(j);
						}
						otherSoftwareSB.append(flag);
					}
				}else{
					int[] otherSoftwareArray = new int[OTHERSOFTWARE_NUM];
					if(indexHashMap.containsKey("osw")){
						String otherSoftware = indexHashMap.get("osw");
						String[] strs = otherSoftware.split(",");
						for(int i = 0; i < strs.length; i++){
							if(strs[i].matches("^[1-9]$") || strs[i].matches("^[1][01]$")){
								otherSoftwareArray[Integer.parseInt(strs[i])-1] = 1;
							}
						}
					}
					for(int j = 0; j < otherSoftwareArray.length; j++){
						if(j > 0){
							otherSoftwareSB.append("\t");
						}
						otherSoftwareSB.append(otherSoftwareArray[j]);
					}	
				}
				
				//modifyHistory
				if(indexHashMap.containsKey("mh")){
					modifyHistory = indexHashMap.get("mh");					
				}
				
				//taskCount
				if(indexHashMap.containsKey("tc")){
					taskCount = indexHashMap.get("tc");					
				}
				
				//lastCrash
				if(indexHashMap.containsKey("lc")){
					lastCrash = indexHashMap.get("lc");					
				}
				
				//maxDownload
				if(indexHashMap.containsKey("md")){
					maxDownload = indexHashMap.get("md");					
				}		
				
				//totalHigh
				if(indexHashMap.containsKey("th")){
					totalHigh = indexHashMap.get("th");					
				}	
				
				//totalLow
				if(indexHashMap.containsKey("tl")){
					totalLow = indexHashMap.get("tl");					
				}	
					
				StringBuilder formatStr = new StringBuilder();	
				
				formatStr.append(date).append("\t");
				formatStr.append(versionId).append("\t");
				formatStr.append(provinceId).append("\t");
				formatStr.append(macFormat).append("\t");
				formatStr.append(ispId).append("\t");
				formatStr.append(channelId).append("\t");
				formatStr.append(recordId).append("\t");
				formatStr.append(runCount).append("\t");
				formatStr.append(otherSoftwareSB).append("\t");
				formatStr.append(modifyHistory).append("\t");
				formatStr.append(taskCount).append("\t");
				formatStr.append(lastCrash).append("\t");
				formatStr.append(maxDownload).append("\t");
				formatStr.append(totalHigh).append("\t");
				formatStr.append(totalLow).append("\t");
				formatStr.append(longIp).append("\t");			
				formatStr.append(timestamp);
				
				context.write(NullWritable.get(), new Text(formatStr.toString()));
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("error originalData: " + originLog);
			}
			
		}
		
	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();
		
		Job job = new Job(conf, "ClientUninstall");
		String statDate = conf.get("stat_date");
		conf.set("stat_date", statDate);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		Path output = new Path(conf.get("output_dir"));
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
		
		job.setJarByClass(UninstallLogFormatMR.class);
		job.setMapperClass(ClientUninstallFormatMapper.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int nRet = ToolRunner.run(new Configuration(), new UninstallLogFormatMR(), args);
		System.out.println(nRet);
	}
}
