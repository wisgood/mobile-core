package com.bi.format;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

public class InstallLogFormatMR extends Configured implements Tool {
	
	public static class ClientInstallFormatMapper extends
			Mapper<LongWritable, Text, NullWritable, Text>{
		private static final int OTHERSOFTWARE_NUM = 11;
		private static String dateId = null;
		private IpParser ipParser = null;
		private ChannelIdInfo channelIdInfo = null;
		private HashMap<String, String> qudaoIdMap = null;
		private List<String> otherSoftwareList = Arrays.asList("ps","thd","pl","qy","pp","lt","ql","sv","bd","qb","bq");   //al:alexa统计，iu:艾瑞统计
		private HashSet<String> installTypeSet = new HashSet<String>(){{
			add("first");
			add("update");
			add("replace");
			add("unknown");
		}};
		private static Logger logger = Logger.getLogger(InstallLogFormatMR.class.getName());
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
		private final String DEFAULT_IDN = "0";
		private final String DEFAULT_INSTALL_TYPE = "unknown";
		private final String DEFAULT_USER_ID = "0";
		private final String DEFAULT_MD5V = "-";
		private final long DEFAULT_BEFORE_VERSION_ID = -999;
		private final String DEFAULT_AUTO = "-999";
		private final String DEFAULT_MODIFY_HISTORY = "-";
		private final String DEFAULT_GUID = "00000000000000000000000000000000";
		private final String DEFAULT_HOME_PAGE = "-";
		private final String DEFAULT_REPAIR = "-999";
		private final String DEFAULT_INSTALL_MODE = "-";
		private final String DEFAULT_UNINSTSCR = "-";
		private final String DEFAULT_OS = "-";
		private final String DEFAULT_ALEXA = "-999";
		private final String DEFAULT_IUSER = "-999";
		private final String DEFAULT_DYT = "-999";
		private final String DEFAULT_YJT = "-999";
		
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
			String idn = DEFAULT_IDN;
			String installType = DEFAULT_INSTALL_TYPE;
			String userId = DEFAULT_USER_ID;
			String timestamp = null;
			String md5v = DEFAULT_MD5V;
			long beforeVersionId = DEFAULT_BEFORE_VERSION_ID;
			String auto = DEFAULT_AUTO;
			String modifyHistory = DEFAULT_MODIFY_HISTORY;
			String guid = DEFAULT_GUID;
			String homePage = DEFAULT_HOME_PAGE;
			String repair = DEFAULT_REPAIR;
			String installMode = DEFAULT_INSTALL_MODE;
			String uninstscr = DEFAULT_UNINSTSCR;
			String os = DEFAULT_OS;
			String alexa = DEFAULT_ALEXA;
			String iuser = DEFAULT_IUSER;
			String dyt = DEFAULT_DYT;
			String yjt = DEFAULT_YJT;
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
				if(indexHashMap.containsKey("id")){
					qudaoId = indexHashMap.get("id");
					if(!qudaoIdMap.containsKey(qudaoId)){
						qudaoId = DEFAULT_QUDAO_ID;
					}
				}
				
				//versionId
				if(indexHashMap.containsKey("v")){
					String version = indexHashMap.get("v");
					versionId = IpParser.ip2long(version);
					if(versionId == 0){
						versionId = DEFAULT_VERSION_ID;
					}
				}
				
				//mac_code
				if(indexHashMap.containsKey("s")){
					String mac = indexHashMap.get("s");
					macCode = MACFormat.macFormat(mac);
				}
				
				//idn
				if(indexHashMap.containsKey("idn")){
					idn = indexHashMap.get("idn");
					if(!qudaoIdMap.containsKey(idn)){
						idn = DEFAULT_IDN;
					}
				}
				
				//install_type
				if(indexHashMap.containsKey("t")){
					installType = indexHashMap.get("t");
					if(!installTypeSet.contains(installType)){
						installType = DEFAULT_INSTALL_TYPE;
					}
				}
				
				//userId
				if(indexHashMap.containsKey("u")){
					userId = indexHashMap.get("u");
					if(!userId.matches("^[0-9]+$")){
						userId = "0"; 
					}
				}
				
				//md5v
				if(indexHashMap.containsKey("c")){
					md5v = indexHashMap.get("c");
				}
				
				//beforeVersionId
				if(indexHashMap.containsKey("ov")){
					String version = indexHashMap.get("ov");
					beforeVersionId = IpParser.ip2long(version);
				}
				
				//auto
				if(indexHashMap.containsKey("auto")){
					auto = indexHashMap.get("auto");
					if(!auto.matches("^[0-9]+$")){
						auto = DEFAULT_AUTO;
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
				
				//homePage
				if(indexHashMap.containsKey("hp")){
					homePage = indexHashMap.get("hp");
					if(!homePage.matches("^[0-9]+$")){
						homePage = DEFAULT_HOME_PAGE;
					}
				}
				
				//repair
				if(indexHashMap.containsKey("rp")){
					repair = indexHashMap.get("rp");
					if(!repair.matches("^[0-9]+$")){
						repair = DEFAULT_REPAIR;
					}
				}
				
				//installMode
				if(indexHashMap.containsKey("im")){
					installMode = indexHashMap.get("im");
				}
				
				//uninstscr
				if(indexHashMap.containsKey("uninstscr")){
					uninstscr = indexHashMap.get("uninstscr");
				}
				
				//os
				if(indexHashMap.containsKey("os")){
					os = indexHashMap.get("os");
				}
				
				//other software
				String[] otherSoftware = new String[OTHERSOFTWARE_NUM];
				for(int i=0; i<OTHERSOFTWARE_NUM; i++){
					otherSoftware[i] = "-999";
				}
				if(indexHashMap.containsKey("other")){
					String other = indexHashMap.get("other");
					String flag = "-999";
					int length = other.length() > OTHERSOFTWARE_NUM ? OTHERSOFTWARE_NUM : other.length();
					for(int j = 0; j < length; j++){
						flag = other.substring(j, j+1);
						if(flag.equals("0") || flag.equals("1")){
							otherSoftware[j] = flag;
						} 
					}
				}else{
					for(int i=0; i < otherSoftwareList.size(); i++){
						String flag = "-999";
						String mapKey = otherSoftwareList.get(i);
						if(indexHashMap.containsKey(mapKey) ){
							flag = indexHashMap.get(mapKey);
							if(flag.equals("0") || flag.equals("1")){
								otherSoftware[i] = flag;
							}
						}
					}
				}
				for(int j = 0; j < OTHERSOFTWARE_NUM; j++){
					if(j > 0){
						otherSoftwareSB.append("\t");
					}
					otherSoftwareSB.append(otherSoftware[j]);
				}
				
				//al
				if(indexHashMap.containsKey("al")){
					alexa = indexHashMap.get("al");
					if(!alexa.equals("0") && !alexa.equals("1")){
						alexa = DEFAULT_ALEXA;
					}
				}
				
				//iuser
				if(indexHashMap.containsKey("iu")){
					iuser = indexHashMap.get("iu");
					if(!iuser.equals("0") && !iuser.equals("1")){
						iuser = DEFAULT_IUSER;
					}
				}
				
				//dyt
				if(indexHashMap.containsKey("DYT")){
					dyt = indexHashMap.get("DYT");
					if(!dyt.matches("^[0-9]+$")){
						dyt = DEFAULT_DYT;
					}
				}
				
				//yjt
				if(indexHashMap.containsKey("YJT")){
					yjt = indexHashMap.get("YJT");
					if(!yjt.matches("^[0-9]+$")){
						yjt = DEFAULT_YJT;
					}
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
						.append(idn).append(DEFAULT_TAB_SEP)
						.append(installType).append(DEFAULT_TAB_SEP)
						.append(userId).append(DEFAULT_TAB_SEP)
						.append(md5v).append(DEFAULT_TAB_SEP)
						.append(beforeVersionId).append(DEFAULT_TAB_SEP)
						.append(auto).append(DEFAULT_TAB_SEP)
						.append(modifyHistory).append(DEFAULT_TAB_SEP)
						.append(guid).append(DEFAULT_TAB_SEP)
						.append(homePage).append(DEFAULT_TAB_SEP)
						.append(repair).append(DEFAULT_TAB_SEP)
						.append(installMode).append(DEFAULT_TAB_SEP)
						.append(uninstscr).append(DEFAULT_TAB_SEP)
						.append(os).append(DEFAULT_TAB_SEP)
						.append(otherSoftwareSB).append(DEFAULT_TAB_SEP)
						.append(alexa).append(DEFAULT_TAB_SEP)
						.append(iuser).append(DEFAULT_TAB_SEP)
						.append(dyt).append(DEFAULT_TAB_SEP)
						.append(yjt).append(DEFAULT_TAB_SEP)
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
