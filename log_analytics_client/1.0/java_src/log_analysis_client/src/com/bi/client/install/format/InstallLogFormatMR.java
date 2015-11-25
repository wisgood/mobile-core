package com.bi.client.install.format;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

public class InstallLogFormatMR extends Configured implements Tool {
	
	public static class ClientInstallFormatMapper extends
			Mapper<LongWritable, Text, NullWritable, Text>{
		private static final int OTHERSOFTWARE_NUM = 11;
		private static String date = null;
		private IpParser1 ipParser = null;
		private List<String> otherSoftwareList = Arrays.asList("ps","thd","pl","qy","pp","lt","ql","sv","bd","qb","bq");   //al:alexa统计，iu:艾瑞统计
		private Set<String> installTypeSet = new HashSet<String>(){{
				add("first");
				add("update");
				add("replace");
				add("unknown");
		}};
		private static Logger logger = Logger.getLogger(InstallLogFormatMR.class.getName());
		
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
			String ipOrigin = null;
			String macOrigin = "-";
			String timestamp = "-";			
			String versionOrigin = null;
			String channelId = "1";
			String installType = "unknown";
			String auto = "-";
			String iu = "0";
			String al = "0";
			String idn = "-1";
			StringBuilder otherSoftwareSB = new StringBuilder();
			
			try {
				String[] fields = originLog.split("&");                 //ip=&u=&time=&s=
				for(int i=0; i < fields.length; i++){
					String patternStr = "^(.+)=(.+)$";
					if(Pattern.matches(patternStr, fields[i])){
						String[] index = fields[i].split("=");
						indexHashMap.put(index[0],index[1]);
					}
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
				String macFormat = MACFormat1.macFormat(macOrigin);
				
				//versionId
				if(indexHashMap.containsKey("v")){
					versionOrigin = indexHashMap.get("v");
				}
				long versionId = 0l;
				versionId = IpParser1.ip2long(versionOrigin);

				//channelId
				if(indexHashMap.containsKey("id")){
					channelId = indexHashMap.get("id");
					if(!Pattern.matches("^\\d{1,6}$", channelId)){
					channelId = "1";
					}
				}
				
//				//idn
//				if(indexHashMap.containsKey("idn")){
//					idn = indexHashMap.get("idn");
//					if(!Pattern.matches("^\\d+$", idn)){
//						idn = "-1";
//					}
//				}

					
				//install_type
				if(indexHashMap.containsKey("t")){
					installType = indexHashMap.get("t");
					if(!installTypeSet.contains(installType)){
						installType= "unknown";
					}	
				}
				
				//is_auto
				if(indexHashMap.containsKey("auto")){
					auto = indexHashMap.get("auto");
				}
				
				//other software
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
					for(int i=0; i < otherSoftwareList.size(); i++){
						String flag = "0";
						if(i > 0){
							otherSoftwareSB.append("\t");
						}
						String mapKey = otherSoftwareList.get(i);
						if(indexHashMap.containsKey(mapKey) ){
							flag = indexHashMap.get(mapKey);							
						}
						otherSoftwareSB.append(flag);
					}
				}
				
				//al
				if(indexHashMap.containsKey("al")){
					al = indexHashMap.get("al");
				}
				
				//iu
				if(indexHashMap.containsKey("iu")){
					iu = indexHashMap.get("iu");
				}

				StringBuilder formatStr = new StringBuilder();	
				
				formatStr.append(date).append("\t");
				formatStr.append(versionId).append("\t");
				formatStr.append(provinceId).append("\t");
				formatStr.append(macFormat).append("\t");
				formatStr.append(ispId).append("\t");
				formatStr.append(channelId).append("\t");
				formatStr.append(installType).append("\t");
				formatStr.append(auto).append("\t");
				formatStr.append(otherSoftwareSB).append("\t");
				formatStr.append(al).append("\t");
				formatStr.append(iu).append("\t");
				formatStr.append(longIp).append("\t");			
				formatStr.append(timestamp);
//				formatStr.append(idn);
				
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
		
		Job job = new Job(conf, "ClientInstallFormat");
		String statDate = conf.get("stat_date");
		conf.set("stat_date", statDate);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		Path output = new Path(conf.get("output_dir"));
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
		
		job.setJarByClass(InstallLogFormatMR.class);
		job.setMapperClass(ClientInstallFormatMapper.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int nRet = ToolRunner.run(new Configuration(), new InstallLogFormatMR(), args);
		System.out.println(nRet);
	}
}
