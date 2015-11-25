package com.bi.log.pv.format.correctdata;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.common.util.SystemInfoUtil;
import com.bi.common.util.TimestampFormatUtil;
import com.bi.common.init.ConstantEnum;

import com.bi.log.pv.format.dataenum.PvEnum;
import com.bi.log.pv.format.dataenum.PvFormatEnum;
import com.bi.common.dm.pojo.dao.AbstractDMDAO;
import com.bi.common.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMInterURLDAOObj;
import com.bi.common.dm.pojo.dao.DMOuterURLRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMKeywordRuleDAOImpl;



public class PvFormatMR {

	public static class PvFormatMap extends Mapper<LongWritable, Text, Text, Text> {

		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;
		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmOuterURLRuleDAO = null;
		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmKeywordRuleDAO = null;
		
		private DMInterURLDAOObj dmInterURLRuleDAO = null;
		
		private static Logger logger = Logger.getLogger(PvFormatMap.class
				.getName());
		private static final String urlSeparator = "http://";
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
			this.dmOuterURLRuleDAO = new DMOuterURLRuleDAOImpl<String, Map<ConstantEnum, String>>();
			this.dmKeywordRuleDAO = new DMKeywordRuleDAOImpl<String, Map<ConstantEnum, String>>();
			this.dmInterURLRuleDAO = new DMInterURLDAOObj();
					
			if (this.isLocalRunMode(context)) {
				String dmIpTableFilePath = context.getConfiguration().get(
						ConstantEnum.IPTABLE_FILEPATH.name());
				this.dmIPRuleDAO.parseDMObj(new File(dmIpTableFilePath));
				
				String dmOuterUrlTableFilePath = context.getConfiguration().get(
						ConstantEnum.DM_OUTER_URL_FILEPATH.name());
				this.dmOuterURLRuleDAO.parseDMObj(new File(dmOuterUrlTableFilePath));
				
				String dmInterUrlTableFilePath = context.getConfiguration().get(
						ConstantEnum.DM_INTER_URL_FILEPATH.name());
				this.dmInterURLRuleDAO.parseDMObj(new File(dmInterUrlTableFilePath));
				
				String dmKeywordTableFilePath = context.getConfiguration().get(
						ConstantEnum.DM_URL_KEYWORD_FILEPATH.name());
				this.dmKeywordRuleDAO.parseDMObj(new File(dmKeywordTableFilePath));
							
				
			} else {
				logger.info("TextMap rummode is cluster!");
				this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE
						.name().toLowerCase()));
				this.dmOuterURLRuleDAO.parseDMObj(new File(ConstantEnum.DM_OUTER_URL
						.name().toLowerCase()));
				this.dmInterURLRuleDAO.parseDMObj(new File(ConstantEnum.DM_INTER_URL
						.name().toLowerCase()));
				this.dmKeywordRuleDAO.parseDMObj(new File(ConstantEnum.DM_URL_KEYWORD
						.name().toLowerCase()));				
			}
		}

		private boolean isLocalRunMode(Context context) {
			String mapredJobTrackerMode = context.getConfiguration().get(
					"mapred.job.tracker");
			if (null != mapredJobTrackerMode
					&& ConstantEnum.LOCAL.name().equalsIgnoreCase(
							mapredJobTrackerMode)) {
				return true;
			}
			return false;
		}
		
	

		public String getPvFormatStr(String originalData) {
			// Text Text = null;
			StringBuilder exitETLSB = new StringBuilder();
			String[] splitSts = originalData.split("\t");
			
			if (splitSts.length <= PvEnum.SEIDCOUNT.ordinal()) {
				return null;
			}
			// System.out.println("length =" + splitSts.length);
			
			try {			
				//System.out.println(originalData);
				
				long tStart = System.currentTimeMillis(); 
				
				String timestampInfoStr = splitSts[PvEnum.TIMESTAMP.ordinal()];
				java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
						.formatTimestamp(timestampInfoStr);

				// dateId
				String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
				String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);

				// hourid
				int hourId = Integer.parseInt(hourIdStr);
				
				// System.out.println("dateId:" + dateId);
				// System.out.println("hourId:" + hourId);
                  
					
				String versionInfo = splitSts[PvEnum.VERSION.ordinal()];
				// System.out.println(versionInfo);
				// versionId
				long versionId = -0l;
				versionId = IPFormatUtil.ip2long(versionInfo);
				
				// System.out.println("versionId:" + versionId);
			
				// ipinfo
				String ipInfoStr = splitSts[PvEnum.IP.ordinal()];
				long ipLong = 0;
				ipLong = IPFormatUtil.ip2long(ipInfoStr);
				java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
						.getDMOjb(ipLong);
				String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
				String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
				
				// System.out.println("ipinfo:" + ipRuleMap);
				
				
				// userFlag parser from fck
				String fckInfoStr = splitSts[PvEnum.FCK.ordinal()];
				
				String fckTimestampInfoStr = "";
				int userFlag = 0;
				if(fckInfoStr.length() == 15){
					fckTimestampInfoStr = fckInfoStr.substring(0, 10);
					try{
						String registerDateId = TimestampFormatUtil.formatTimeStamp(fckTimestampInfoStr,"yyyyMMdd");
						if(registerDateId.equals(dateId))
							userFlag = 1;
					}
					catch(Exception e ){
						userFlag = 0;
					}	
				}
				// urlinfo parser
				String urlInfoStrRegion = splitSts[PvEnum.URL.ordinal()];
				// System.out.println("urlInfoStrRegion = "+ urlInfoStrRegion );
				String urlInfoStr = StringDecodeFormatUtil.decodeCodedStr(urlInfoStrRegion , "utf-8");
				
				java.util.Map<ConstantEnum, String> URLTypeMap = this.dmInterURLRuleDAO
						.getDMOjb(urlInfoStr);
				
				String urlFirstId = URLTypeMap.get(ConstantEnum.URL_FIRST_ID).trim();
				String urlSecondId = URLTypeMap.get(ConstantEnum.URL_SECOND_ID).trim();
				String urlThirdId = URLTypeMap.get(ConstantEnum.URL_THIRD_ID).trim();
				String urlKeyword = "";
				String urlPage = "";
				if(URLTypeMap.size()>3){
					urlKeyword = URLTypeMap.get(ConstantEnum.URL_KEYWORD).trim();
					urlPage = URLTypeMap.get(ConstantEnum.URL_PAGE).trim();
				}
				
				// logger.info("urlinfo:" + URLTypeMap);	
				
				// referurlinfo parser
				String referUrlInfoRegion = splitSts[PvEnum.REFERURL.ordinal()];
				String referUrlInfoStr = referUrlInfoRegion;
				String referUrlFirstId = "";
				String referUrlSecondId = "";
				String referUrlThirdId = "";
				String referUrlKeyword = "";
				String referUrlPage = "";
				
				if(referUrlInfoRegion.contains(urlSeparator)){
					String referUrlInfoArr[] = referUrlInfoRegion.split(urlSeparator);
					referUrlInfoStr = urlSeparator.concat(referUrlInfoArr[1]);
				}
				
				// System.out.println("referUrlInfoStr =" + referUrlInfoStr);
				if(referUrlInfoStr.contains(".funshion.com"))
				{
					URLTypeMap = this.dmInterURLRuleDAO.getDMOjb(referUrlInfoStr);
					referUrlFirstId = URLTypeMap.get(ConstantEnum.URL_FIRST_ID).trim();
					referUrlSecondId = URLTypeMap.get(ConstantEnum.URL_SECOND_ID).trim();
					referUrlThirdId = URLTypeMap.get(ConstantEnum.URL_THIRD_ID).trim();
					if(URLTypeMap.size()>3){
						referUrlKeyword = URLTypeMap.get(ConstantEnum.URL_KEYWORD).trim();
						referUrlPage = URLTypeMap.get(ConstantEnum.URL_PAGE).trim();
					}
					// logger.info("referurlinfo:" + URLTypeMap);		
				}
				else{
					java.util.Map<ConstantEnum, String> referUrlRuleMap = this.dmOuterURLRuleDAO
						.getDMOjb(referUrlInfoStr);
				
					referUrlFirstId = referUrlRuleMap.get(ConstantEnum.URL_FIRST_ID).trim();
					referUrlSecondId = referUrlRuleMap.get(ConstantEnum.URL_SECOND_ID).trim();
					referUrlThirdId = referUrlRuleMap.get(ConstantEnum.URL_THIRD_ID).trim();
					// logger.info("referurlinfo:" + referUrlRuleMap);
				
					//referurl searchkeyword parser
					java.util.Map<ConstantEnum, String> keywordRuleMap = this.dmKeywordRuleDAO
						.getDMOjb(referUrlInfoStr);
					referUrlKeyword = keywordRuleMap.get(ConstantEnum.URL_KEYWORD).trim();
					// logger.info("searchkeywordinfo:" + keywordRuleMap);
				}
				
				// mac地址
				String macInfoStr = splitSts[PvEnum.MAC.ordinal()];
				MACFormatUtil.isCorrectMac(macInfoStr);
//				String macCode = MACFormatUtil.macFormat(macInfoStr).get(
//						ConstantEnum.MAC_LONG);
				String macInfor = MACFormatUtil.macFormatToCorrectStr(macInfoStr);
				// System.out.println("MacCode:" + macInfor);
				// browseInfo & systemInfo parser
				String userAgentStr = splitSts[PvEnum.USERAGENT.ordinal()];
				
				// System.out.println("userAgentStr = " + userAgentStr);
				java.util.Map<ConstantEnum, String> sysAndbroMap = 
						SystemInfoUtil.getBroAndOSInfo(userAgentStr);
				String browseInfo = "";	
				String systemInfo = "";
						
				if(userAgentStr != null && !"".equals(userAgentStr)){
					browseInfo = sysAndbroMap.get(ConstantEnum.BROWSE_INFO).trim();
					systemInfo = sysAndbroMap.get(ConstantEnum.SYSTEM_INFO).trim();
				}
				
				exitETLSB.append(dateId + "\t");
				exitETLSB.append(hourId + "\t");
				exitETLSB.append(versionId + "\t");
				exitETLSB.append(provinceId + "\t");
				exitETLSB.append(cityId + "\t");
				exitETLSB.append(macInfor + "\t");
				exitETLSB.append(userFlag + "\t");
				
				exitETLSB.append(urlFirstId + "\t");
				exitETLSB.append(urlSecondId + "\t");
				exitETLSB.append(urlThirdId + "\t");
				exitETLSB.append(urlKeyword + "\t");
				exitETLSB.append(urlPage + "\t");
				
				exitETLSB.append(referUrlFirstId + "\t");
				exitETLSB.append(referUrlSecondId + "\t");
				exitETLSB.append(referUrlThirdId + "\t");
				exitETLSB.append(referUrlKeyword  + "\t");
				exitETLSB.append(referUrlPage  + "\t");
				
				exitETLSB.append(browseInfo  + "\t");
				exitETLSB.append(systemInfo  + "\t");
	
				exitETLSB.append(originalData.trim());

			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("error originalData:"+originalData);
				logger.error(e.getMessage(), e.getCause());
			}

			return exitETLSB.toString();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String pvETLStr = this.getPvFormatStr(line);
			if (null != pvETLStr && !("".equalsIgnoreCase(pvETLStr))) {
				context.write(new Text(
						pvETLStr.split("\t")[PvFormatEnum.TIMESTAMP.ordinal()]),
						new Text(pvETLStr));
			}
		}
	}

	public static class PvFormatReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, new Text());
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(PvFormatMR.class);
		job.setJobName("pVFormatMR");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		
		// 设置配置文件默认路径
		job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(),
				"conf/ip_table");
		job.getConfiguration().set(
				ConstantEnum.DM_OUTER_URL_FILEPATH.name(),
				"conf/dm_outer_url");
		
		job.getConfiguration().set(
				ConstantEnum.DM_INTER_URL_FILEPATH.name(),
				"conf/dm_inter_url");
		
		job.getConfiguration().set(
				ConstantEnum.DM_URL_KEYWORD_FILEPATH.name(),
				"conf/dm_url_keyword");
		
		FileInputFormat.addInputPath(job, new Path("input_pv"));
		FileOutputFormat.setOutputPath(job, new Path("output_pv"));
		job.setMapperClass(PvFormatMap.class);
		job.setReducerClass(PvFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
