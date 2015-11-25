package com.bi.format;

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

import com.bi.client.constantEnum.ConstantEnum;
import com.bi.client.util.ChannelIdInfo;
import com.bi.client.util.IpParser;
import com.bi.client.util.MACFormat;
import com.bi.client.util.TimeFormat;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class UninstallLogFormatMR extends Configured implements Tool {
	
	public static class ClientUninstallFormatMapper extends
			Mapper<LongWritable, Text, NullWritable, Text>{
		private static final int OTHERSOFTWARE_NUM = 11;
		private static String dateId = null;
		private IpParser ipParser = null;
		private ChannelIdInfo channelIdInfo = null;
		private HashMap<String, String> qudaoIdMap = null;
		private static Logger logger = Logger.getLogger(UninstallLogFormatMR.class.getName());
		private final String DEFAULT_TAB_SEP = "\t";
		private final String DEFAULT_HOUR_ID = "0";
		private final String DEFAULT_PROVINCE_ID = "-999";
		private final String DEFAULT_CITY_ID = "-999";
		private final String DEFAULT_ISP_ID = "-999";
		private final String DEFAULT_PLAT_ID = "2";
		private final String DEFAULT_QUDAO_ID = "1";
		private final long DEFAULT_VERSION_ID = -999;
		private final String DEFAULT_IP = "0.0.0.0";
		private final String DEFAULT_MAC_CODE = "000000000000";
		private final String DEFAULT_RECORD_ID = "0000000000000";
		private final String DEFAULT_MD5V = "-";
		private final String DEFAULT_RUN_COUNT = "-999";
		private final String DEFAULT_TASK_COUNT = "-999";
		private final String DEFAULT_LAST_CRASH = "-";
		private final String DEFAULT_MAX_DOWNLOAD = "-999";
		private final String DEFAULT_TOTAL_HIGH = "-999";
		private final String DEFAULT_TOTAL_LOW = "-999";
		private final String DEFAULT_MODIFY_HISTORY = "-";
		private final String DEFAULT_GUID = "00000000000000000000000000000000";
		private final String DEFAULT_UNINSTSCR = "-";
		public void setup(Context context) throws IOException, InterruptedException{
			ipParser = new IpParser();
			channelIdInfo = new ChannelIdInfo();
			try{
				ipParser.init("ip_table");
				channelIdInfo.init("dm_client_channel_dimension");
				qudaoIdMap = channelIdInfo.getChannelIdMap();
				Configuration conf = context.getConfiguration();
				dateId = conf.get("stat_date");
			}catch(IOException e){
				e.printStackTrace();
				System.exit(0);
			}	
		}
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String originLog = value.toString();
			HashMap<String, String> indexHashMap = new HashMap<String, String>();
			
			String hourId = DEFAULT_HOUR_ID;
			String provinceId = DEFAULT_PROVINCE_ID;
			String cityId = DEFAULT_CITY_ID;
			String ispId = DEFAULT_ISP_ID;
			String platId = DEFAULT_PLAT_ID;
			String qudaoId = DEFAULT_QUDAO_ID;
			long versionId = DEFAULT_VERSION_ID;
			String ip = DEFAULT_IP;
			String macCode = DEFAULT_MAC_CODE;
			String recordId = DEFAULT_RECORD_ID;
			String md5v = DEFAULT_MD5V;
			String runCount = DEFAULT_RUN_COUNT;
			String taskCount = DEFAULT_TASK_COUNT;
			String lastCrash = DEFAULT_LAST_CRASH;
			String maxDownload = DEFAULT_MAX_DOWNLOAD;
			String totalHigh = DEFAULT_TOTAL_HIGH;
			String totalLow = DEFAULT_TOTAL_LOW;
			String modifyHistory = DEFAULT_MODIFY_HISTORY;
			String guid = DEFAULT_GUID;
			String uninstscr = DEFAULT_UNINSTSCR;
			String timestamp = null;
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
				
				//date_id,hour_id
				if(indexHashMap.containsKey("time")){
					timestamp = indexHashMap.get("time");					
					String timeStampStr = TimeFormat.toString(timestamp);
					String[] times = timeStampStr.split("\t");
					dateId = times[1];
					hourId = times[2];
				}
				
				//ip,province_id,city_id,isp_id
				if(indexHashMap.containsKey("ip")){
					ip = IpParser.ipVerify(indexHashMap.get("ip"));
					long ipLong = IpParser.ip2long(ip);
					HashMap<ConstantEnum, String> areaMap = ipParser.getAreaMap(ipLong);
					provinceId = areaMap.get(ConstantEnum.PROVINCE_ID);
					cityId = areaMap.get(ConstantEnum.CITY_ID);
					ispId = areaMap.get(ConstantEnum.ISP_ID);
				}
				
				//qudaoId
				if(indexHashMap.containsKey("i")){
					qudaoId = indexHashMap.get("i");
					if(!qudaoIdMap.containsKey(qudaoId)){
						qudaoId = DEFAULT_QUDAO_ID;
					}
				}
				
				//versionId
				if(indexHashMap.containsKey("v")){
					String version = indexHashMap.get("v");
					versionId = IpParser.ip2long(version);
					if(versionId == 0)
						versionId = DEFAULT_VERSION_ID;
				}
				
				//mac_code
				if(indexHashMap.containsKey("s")){
					String mac = indexHashMap.get("s");
					macCode = MACFormat.macFormat(mac);
				}
				
				//recordId
				if(indexHashMap.containsKey("record_id")){
					recordId = indexHashMap.get("record_id");
					if(!recordId.matches("^[0-9]+$")){
						recordId = DEFAULT_RECORD_ID;
					}
				}
				
				//md5v
				if(indexHashMap.containsKey("c")){
					md5v = indexHashMap.get("c");
				}
				
				//runCount
				if(indexHashMap.containsKey("r")){
					runCount= indexHashMap.get("r");
					if(!runCount.matches("^[0-9]+$")){
						runCount = DEFAULT_RUN_COUNT;
					}
				}
				
				//otherSoftware
				String[] otherSoftware = new String[OTHERSOFTWARE_NUM];
				for(int i=0; i<OTHERSOFTWARE_NUM; i++){
					otherSoftware[i] = "-999";
				}
				if(indexHashMap.containsKey("other")){
					String other = indexHashMap.get("other");
					if(other.matches("^[01]+$")){
						int length = other.length() > OTHERSOFTWARE_NUM ? OTHERSOFTWARE_NUM : other.length();
						for(int j = 0; j < length; j++){
							otherSoftware[j] = other.substring(j, j+1);
						}
					}
				}else if(indexHashMap.containsKey("osw")){
					String other = indexHashMap.get("osw");
					String[] strs = other.split(",");
					boolean flag = false;
					String[] others = new String[OTHERSOFTWARE_NUM];
					for(int m=0; m<OTHERSOFTWARE_NUM; m++){
						others[m] = "0";
					}
					for(int i = 0; i < strs.length; i++){
						if(strs[i].matches("^[1-9]$") || strs[i].matches("^[1][01]$")){
							others[Integer.parseInt(strs[i])-1] = "1";
							flag = true;
						}
					}
					if(flag){               //only when the osw's value is correct, the othersoftware's value is 0. eg.osw=15,the value should be -999
						otherSoftware = others;
					}
				}
				for(int j = 0; j < OTHERSOFTWARE_NUM; j++){
					if(j > 0){
						otherSoftwareSB.append("\t");
					}
					otherSoftwareSB.append(otherSoftware[j]);
				}
				
				//taskCount
				if(indexHashMap.containsKey("tc")){
					taskCount = indexHashMap.get("tc");
					if(!taskCount.matches("^[0-9]+$")){
						taskCount = DEFAULT_TASK_COUNT;
					}
				}
				
				//lastCrash
				if(indexHashMap.containsKey("lc")){
					lastCrash = indexHashMap.get("lc");					
				}
				
				//maxDownload
				if(indexHashMap.containsKey("md")){
					maxDownload = indexHashMap.get("md");
					if(!maxDownload.matches("^[0-9]+$")){
						maxDownload = DEFAULT_MAX_DOWNLOAD;
					}
				}
				
				//totalHigh
				if(indexHashMap.containsKey("th")){
					totalHigh = indexHashMap.get("th");
					if(!totalHigh.matches("^[0-9]+$")){
						totalHigh = DEFAULT_TOTAL_HIGH;
					}
				}
				
				//totalLow
				if(indexHashMap.containsKey("tl")){
					totalLow = indexHashMap.get("tl");
					if(!totalLow.matches("^[0-9]+$")){
						totalLow = DEFAULT_TOTAL_LOW;
					}
				}
				
				//modifyHistory
				if(indexHashMap.containsKey("mh")){
					modifyHistory = indexHashMap.get("mh");					
				}
				
				//guid
				if(indexHashMap.containsKey("guid")){
					guid = indexHashMap.get("guid");
				}
				
				//uninstscr
				if(indexHashMap.containsKey("uninstscr")){
					uninstscr = indexHashMap.get("uninstscr");
				}
					
				StringBuilder formatStr = new StringBuilder();	
				
				formatStr.append(dateId).append(DEFAULT_TAB_SEP)
				.append(hourId).append(DEFAULT_TAB_SEP)
				.append(provinceId).append(DEFAULT_TAB_SEP)
				.append(cityId).append(DEFAULT_TAB_SEP)
				.append(ispId).append(DEFAULT_TAB_SEP)
				.append(platId).append(DEFAULT_TAB_SEP)
				.append(qudaoId).append(DEFAULT_TAB_SEP)
				.append(versionId).append(DEFAULT_TAB_SEP)
				.append(ip).append(DEFAULT_TAB_SEP)
				.append(macCode).append(DEFAULT_TAB_SEP)
				.append(recordId).append(DEFAULT_TAB_SEP)
				.append(md5v).append(DEFAULT_TAB_SEP)
				.append(runCount).append(DEFAULT_TAB_SEP)
				.append(otherSoftwareSB).append(DEFAULT_TAB_SEP)
				.append(taskCount).append(DEFAULT_TAB_SEP)
				.append(lastCrash).append(DEFAULT_TAB_SEP)
				.append(maxDownload).append(DEFAULT_TAB_SEP)
				.append(totalHigh).append(DEFAULT_TAB_SEP)
				.append(totalLow).append(DEFAULT_TAB_SEP)
				.append(modifyHistory).append(DEFAULT_TAB_SEP)
				.append(guid).append(DEFAULT_TAB_SEP)
				.append(uninstscr).append(DEFAULT_TAB_SEP)
				.append(timestamp);
				
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
		
		Job job = new Job(conf, conf.get("job_name"));
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
