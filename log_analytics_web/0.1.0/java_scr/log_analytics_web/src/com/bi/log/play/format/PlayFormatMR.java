package com.bi.log.play.format;

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

import com.bi.common.dm.constant.DMMediaTypeEnum;
import com.bi.common.dm.pojo.dao.AbstractDMDAO;
import com.bi.common.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMInterURLDAOObj;
import com.bi.common.dm.pojo.dao.DMKeywordRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMOuterURLRuleDAOImpl;
import com.bi.common.init.ConstantEnum;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.MediaInfoUtil;
import com.bi.common.util.TimestampFormatUtil;

public class PlayFormatMR {
	public static class PlayFormatMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;
		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmOuterURLRuleDAO = null;
		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmKeywordRuleDAO = null;

		private DMInterURLDAOObj dmInterURLRuleDAO = null;

		private static Logger logger = Logger.getLogger(PlayFormatMap.class
				.getName());

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

				String dmOuterUrlTableFilePath = context.getConfiguration()
						.get(ConstantEnum.DM_OUTER_URL_FILEPATH.name());
				this.dmOuterURLRuleDAO.parseDMObj(new File(
						dmOuterUrlTableFilePath));

				String dmInterUrlTableFilePath = context.getConfiguration()
						.get(ConstantEnum.DM_INTER_URL_FILEPATH.name());
				this.dmInterURLRuleDAO.parseDMObj(new File(
						dmInterUrlTableFilePath));

				String dmKeywordTableFilePath = context.getConfiguration().get(
						ConstantEnum.DM_URL_KEYWORD_FILEPATH.name());
				this.dmKeywordRuleDAO.parseDMObj(new File(
						dmKeywordTableFilePath));

			} else {
				this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE
						.name().toLowerCase()));
				this.dmOuterURLRuleDAO.parseDMObj(new File(
						ConstantEnum.DM_OUTER_URL.name().toLowerCase()));
				this.dmInterURLRuleDAO.parseDMObj(new File(
						ConstantEnum.DM_INTER_URL.name().toLowerCase()));
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

		public String getPlayFormatStr(String originalData) {
			// Text Text = null;
			StringBuilder playETLSB = new StringBuilder();
			String[] splitStrs = originalData.split("\t");

			if (splitStrs.length <= PlayLogEnum.SEIDCOUNT.ordinal()) {
				return null;
			}
			try {
				String timstampInfoStr = splitStrs[PlayLogEnum.TIMESTAMP
						.ordinal()];
				java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
						.formatTimestamp(timstampInfoStr);
				// dateId
				String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
				String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
				int hourId = Integer.parseInt(hourIdStr);
				String userTypeInfo = "0";
				if(MediaInfoUtil.getUserType(splitStrs[PlayLogEnum.FCK.ordinal()], dateId) == true){
					userTypeInfo = "1";
				}
				String versionInfo = IPFormatUtil.ipFormat(splitStrs[PlayLogEnum.VERSION.ordinal()]);
				//System.out.println(versionInfo);
				// versionId
				long versionId = -0l;
				versionId = IPFormatUtil.ip2long(versionInfo);
				// ipinfo
				String ipInfoStr = IPFormatUtil.ipFormat(splitStrs[PlayLogEnum.IP.ordinal()]);
				long ipLong = 0;
				ipLong = IPFormatUtil.ip2long(ipInfoStr);
				//String ipStr = Long.toString(ipLong);
				java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
						.getDMOjb(ipLong);
				String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
				String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
				// urlinfo parser
				String urlInfoStr = splitStrs[PlayLogEnum.URL.ordinal()];
				Map<ConstantEnum, String> URLTypeMap = this.dmInterURLRuleDAO
						.getDMOjb(urlInfoStr);
				String urlFirstId = URLTypeMap.get(ConstantEnum.URL_FIRST_ID);
				String urlSecondId = URLTypeMap.get(ConstantEnum.URL_SECOND_ID);
				String urlThirdId = URLTypeMap.get(ConstantEnum.URL_THIRD_ID);
				String urlKeyword = "";
				String urlPage = "";
				if (URLTypeMap.size() > 3) {
					urlKeyword = URLTypeMap.get(ConstantEnum.URL_KEYWORD);
					urlPage = URLTypeMap.get(ConstantEnum.URL_PAGE);
				}

				// referurlinfo parser
				String referUrlInfoRegion = splitStrs[PlayLogEnum.REFERURL
						.ordinal()];
				String referUrlInfoStr = referUrlInfoRegion;
				String referUrlFirstId = "";
				String referUrlSecondId = "";
				String referUrlThirdId = "";
				String referUrlKeyword = "";
				String referUrlPage = "";

				if (referUrlInfoRegion.contains("http://")) {
					String referUrlInfoArr[] = referUrlInfoRegion
							.split("http://");
					referUrlInfoStr = "http://".concat(referUrlInfoArr[1]);
				}
				if (referUrlInfoStr.contains(".funshion.com")) {
					URLTypeMap = this.dmInterURLRuleDAO
							.getDMOjb(referUrlInfoStr);
					referUrlFirstId = URLTypeMap.get(ConstantEnum.URL_FIRST_ID);
					referUrlSecondId = URLTypeMap
							.get(ConstantEnum.URL_SECOND_ID);
					referUrlThirdId = URLTypeMap.get(ConstantEnum.URL_THIRD_ID);
					if (URLTypeMap.size() > 3) {
						referUrlKeyword = URLTypeMap.get(ConstantEnum.URL_KEYWORD);
						referUrlPage = URLTypeMap.get(ConstantEnum.URL_PAGE);
					}
				} else {
					Map<ConstantEnum, String> referUrlRuleMap = this.dmOuterURLRuleDAO
							.getDMOjb(referUrlInfoStr);

					referUrlFirstId = referUrlRuleMap
							.get(ConstantEnum.URL_FIRST_ID);
					referUrlSecondId = referUrlRuleMap
							.get(ConstantEnum.URL_SECOND_ID);
					referUrlThirdId = referUrlRuleMap
							.get(ConstantEnum.URL_THIRD_ID);
					// referurl searchkeyword parser
					java.util.Map<ConstantEnum, String> keywordRuleMap = this.dmKeywordRuleDAO
							.getDMOjb(referUrlInfoStr);
					referUrlKeyword = keywordRuleMap
							.get(ConstantEnum.URL_KEYWORD);
				}

				// mac地址
				String macInfoStr = splitStrs[PlayLogEnum.MAC.ordinal()];
				MACFormatUtil.isCorrectMac(macInfoStr);
				// String macCode = MACFormatUtil.macFormat(macInfoStr).get(
				// ConstantEnum.MAC_LONG);
				String macInfor = MACFormatUtil
						.macFormatToCorrectStr(macInfoStr);
				
				String mediaTypeInfo = "";
				String playTypeInfo = "";
				Map<String, String> mediaInfoMap = MediaInfoUtil
						.getMediaType(splitStrs[PlayLogEnum.MEDIATYPE.ordinal()]);
				if (mediaInfoMap != null) {
					mediaTypeInfo = mediaInfoMap.get(DMMediaTypeEnum.MEDIA_TYPE
							.name());
					playTypeInfo = mediaInfoMap.get(DMMediaTypeEnum.PLAY_TYPE
							.name());
				}
				String mediaIDInfo = "";
				String seariaIDInfo = "";
				Map<String, String> mediaIDMap = MediaInfoUtil
						.getMediaID(splitStrs[PlayLogEnum.TARGET.ordinal()]);
				if (mediaInfoMap != null) {
					mediaIDInfo = mediaIDMap.get(DMMediaTypeEnum.MEDIA_ID
							.name());
					seariaIDInfo = mediaIDMap.get(DMMediaTypeEnum.SEARIA_ID
							.name());
				}
				playETLSB.append(dateId + "\t");
				playETLSB.append(hourId + "\t");
				playETLSB.append(provinceId + "\t");
				playETLSB.append(cityId + "\t");
				playETLSB.append(macInfor + "\t");
				playETLSB.append(versionId + "\t");
				playETLSB.append(userTypeInfo + "\t");
				
				playETLSB.append(urlFirstId + "\t");
				playETLSB.append(urlSecondId + "\t");
				playETLSB.append(urlThirdId + "\t");
				playETLSB.append(urlKeyword + "\t");
				playETLSB.append(urlPage + "\t");

				playETLSB.append(referUrlFirstId + "\t");
				playETLSB.append(referUrlSecondId + "\t");
				playETLSB.append(referUrlThirdId + "\t");
				playETLSB.append(referUrlKeyword + "\t");
				playETLSB.append(referUrlPage + "\t");
				
				playETLSB.append(playTypeInfo + "\t");
				playETLSB.append(mediaTypeInfo + "\t");
	
				playETLSB.append(mediaIDInfo + "\t");
				playETLSB.append(seariaIDInfo + "\t");

				playETLSB.append(originalData.trim());

			} catch (Exception e) {
				//System.out.println(e.toString());
				//logger.error("error originalData:" + originalData);
				logger.error(e.getMessage(), e.getCause());
			}
			return playETLSB.toString();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String playETLStr = this.getPlayFormatStr(line);
			if (null != playETLStr && !("".equalsIgnoreCase(playETLStr))) {
				context.write(
						new Text(playETLStr.split("\t")[PlayLogEnum.TIMESTAMP
								.ordinal()]), new Text(playETLStr));
			}
		}
	}

	public static class PlayFormatReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, new Text());
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(PlayFormatMR.class);
		job.setJobName("playFormatMR");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");

		// 设置配置文件默认路径
		job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(),
				"conf/ip_table");
		job.getConfiguration().set(ConstantEnum.DM_OUTER_URL_FILEPATH.name(),
				"conf/dm_outer_url");

		job.getConfiguration().set(ConstantEnum.DM_INTER_URL_FILEPATH.name(),
				"conf/dm_inter_url");

		job.getConfiguration().set(ConstantEnum.DM_URL_KEYWORD_FILEPATH.name(),
				"conf/dm_outer_keyword");

		FileInputFormat.addInputPath(job, new Path("input_pv"));
		FileOutputFormat.setOutputPath(job, new Path("output_pv"));
		job.setMapperClass(PlayFormatMap.class);
		job.setReducerClass(PlayFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
