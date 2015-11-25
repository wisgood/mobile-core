/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: MPVFormatMR.java 
 * @Package com.bi.format 
 * @Description: 对mpv日志名进行处理
 * @author: wanghh
 * @date: 2014-01-11
 * @last modifid date: 2014-1-16-11:30
 * @input: /dw/logs/web/origin/mpv/1
 * @output: /dw/logs/format/mpv/1/
 * @executeCmd: hadoop jar ....
 * @inputFormat: time.......
 * @ouputFormat: DATE_ID,HOUR_ID, PROVINCE_ID,CITY_ID, ISP_ID, PLAT_ID, QUDAO_ID, VERSION_ID, IP, ..
 */
package com.bi.format;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.CommonConstant;
import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DefaultFieldValueEnum;
import com.bi.common.constant.UtilComstrantsEnum;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMKeywordRuleDAOImpl;
import com.bi.common.dimprocess.DMMobileWebURLImpl;
import com.bi.common.dimprocess.DMOuterURLRuleImpl;
import com.bi.common.logenum.MPVEnum;
import com.bi.common.logenum.MPVFormatEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.MediaInfoUtil;
import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.common.util.StringFormatUtil;
import com.bi.common.util.SystemInfoUtil;
import com.bi.common.util.TimestampFormatNewUtil;
import com.bi.common.util.WebCheckUtil;

public class MPVFormatMR extends Configured implements Tool {

	private static final String DEFAULT_NEGATIVE_NUM = "-999";

	public static class MPVFormatMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private static final String urlSeparator = "http://";

		private String dateId = null;

		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmOuterURLRuleDAO = null;

		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmKeywordRuleDAO = null;

		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmMobileURLRuleDAO = null;

		private MultipleOutputs<Text, Text> multipleOutputs;

		private String filePath = null;

		private Text keyText = null;

		private Text valueText = null;

		protected void setup(Context context) throws IOException,
				InterruptedException {

			super.setup(context);

			this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();

			this.dmOuterURLRuleDAO = new DMOuterURLRuleImpl<String, Map<ConstantEnum, String>>();

			this.dmKeywordRuleDAO = new DMKeywordRuleDAOImpl<String, Map<ConstantEnum, String>>();

			this.dmMobileURLRuleDAO = new DMMobileWebURLImpl<String, Map<ConstantEnum, String>>();

			this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
					.toLowerCase()));

			this.dmOuterURLRuleDAO.parseDMObj(new File(
					ConstantEnum.DM_OUTER_URL.name().toLowerCase()));
			this.dmMobileURLRuleDAO.parseDMObj(new File(
					ConstantEnum.DM_MOBILE_WEB_INTER_URL.name().toLowerCase()));
			this.dmKeywordRuleDAO.parseDMObj(new File(
					ConstantEnum.DM_URL_KEYWORD_2.name().toLowerCase()));
			dateId = context.getConfiguration().get("dateid");

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			filePath = fileSplit.getPath().getParent().toString();

			keyText = new Text();
			valueText = new Text();
		}

		public String getMpvFormatStr(String originalData) throws Exception {

			StringBuilder mpvFormatStr = new StringBuilder();
			String[] rawsplitStr = StringFormatUtil.splitLog(originalData,
					StringFormatUtil.TAB_SEPARATOR);

			String buildData = originalData;

			if (rawsplitStr.length < MPVEnum.TA.ordinal() + 1) {
				StringBuilder rawData = new StringBuilder(originalData);
				for (int i = rawsplitStr.length; i < MPVEnum.TA.ordinal() + 1; i++) {
					rawData.append(UtilComstrantsEnum.tabSeparator
							.getValueStr() + DEFAULT_NEGATIVE_NUM);
				}
				buildData = rawData.toString();
			}
			String[] splitStr = StringFormatUtil.splitLog(buildData,
					StringFormatUtil.TAB_SEPARATOR);
			try {

				String timestampInfo = splitStr[MPVEnum.TIMESTAMP.ordinal()]
						.trim();
				String dateIdAndhourId = TimestampFormatNewUtil
						.formatTimestamp(timestampInfo, dateId);

				String versionInfo = WebCheckUtil.checkField(
						splitStr[MPVEnum.VERSION.ordinal()], "0.0.0.0");
				long versionId = -1;
				versionId = IPFormatUtil.ip2long(versionInfo);

				String ipInfoStr = WebCheckUtil.checkField(
						splitStr[MPVEnum.IP.ordinal()], "0.0.0.0");
				long ipLong = 0;
				ipLong = IPFormatUtil.ip2long(ipInfoStr);
				java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
						.getDMOjb(ipLong);
				String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
				String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
				String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

				String fckStr = splitStr[MPVEnum.FCK.ordinal()];
				String fckInfo = "";
				if (true == WebCheckUtil.checkFck(fckStr)) {
					fckInfo = fckStr;
				} else {
					fckInfo = DefaultFieldValueEnum.fckDefault.getValueStr();
				}
				String userTypeInfo = "0";
				if (MediaInfoUtil.getUserType(fckStr, dateId) == true) {
					userTypeInfo = "1";
				}

				String clientflagInfo = WebCheckUtil.checkField(
						splitStr[MPVEnum.CLIENT_FLAG.ordinal()], "4");

				String platId = "11";

				String qudaoId = splitStr[MPVEnum.QUDAO_ID.ordinal()];
				if (WebCheckUtil
						.checkIsNum(splitStr[MPVEnum.QUDAO_ID.ordinal()]) == false) {
					qudaoId = "0";
				}

				String urlInfoRegion = splitStr[MPVEnum.URL.ordinal()];
				String urlInfo = StringDecodeFormatUtil.decodeCodedStr(
						urlInfoRegion, "utf-8").replaceAll("\\s+", "");
				if (urlInfo.isEmpty()) {
					urlInfo = DEFAULT_NEGATIVE_NUM;
				}

				java.util.Map<ConstantEnum, String> URLTypeMap = this.dmMobileURLRuleDAO
						.getDMOjb(urlInfo);

				String urlFirstId = URLTypeMap.get(ConstantEnum.URL_FIRST_ID)
						.trim();
				String urlSecondId = URLTypeMap.get(ConstantEnum.URL_SECOND_ID)
						.trim();
				String urlThirdId = URLTypeMap.get(ConstantEnum.URL_THIRD_ID)
						.trim();
				String urlKeyword = "";
				String urlPage = DEFAULT_NEGATIVE_NUM;
				if (URLTypeMap.size() > 3) {
					urlKeyword = URLTypeMap.get(ConstantEnum.URL_KEYWORD)
							.trim();
					urlPage = URLTypeMap.get(ConstantEnum.URL_PAGE).trim();
				}

				String referUrlInfoRegion = splitStr[MPVEnum.REFERURL.ordinal()];
				String referUrlInfoStr = referUrlInfoRegion.replaceAll("\\s+",
						"");

				String referUrlFirstId = DEFAULT_NEGATIVE_NUM;
				String referUrlSecondId = DEFAULT_NEGATIVE_NUM;
				String referUrlThirdId = DEFAULT_NEGATIVE_NUM;
				String referUrlKeyword = "";
				String referUrlInfo = referUrlInfoStr;
				String referUrlPage = DEFAULT_NEGATIVE_NUM;
				if (referUrlInfoStr.contains(urlSeparator)) {
					String referUrlInfoArr[] = referUrlInfoStr
							.split(urlSeparator);
					referUrlInfo = urlSeparator.concat(referUrlInfoArr[1]);
				}
				String referUrl = referUrlInfo;
				if (referUrlInfo.length() > 200) {
					referUrl = referUrlInfo.substring(0, 200);
				}
				if (referUrl.contains(".funshion.com")) {
					URLTypeMap = this.dmMobileURLRuleDAO.getDMOjb(referUrlInfo);
					referUrlFirstId = URLTypeMap.get(ConstantEnum.URL_FIRST_ID)
							.trim();
					referUrlSecondId = URLTypeMap.get(
							ConstantEnum.URL_SECOND_ID).trim();
					referUrlThirdId = URLTypeMap.get(ConstantEnum.URL_THIRD_ID)
							.trim();
					if (URLTypeMap.size() > 3) {
						referUrlKeyword = URLTypeMap.get(
								ConstantEnum.URL_KEYWORD).trim();
						referUrlPage = URLTypeMap.get(ConstantEnum.URL_PAGE)
								.trim();
					}
				} else {
					java.util.Map<ConstantEnum, String> referUrlRuleMap = this.dmOuterURLRuleDAO
							.getDMOjb(referUrlInfo);

					referUrlFirstId = referUrlRuleMap.get(
							ConstantEnum.URL_FIRST_ID).trim();
					referUrlSecondId = referUrlRuleMap.get(
							ConstantEnum.URL_SECOND_ID).trim();
					referUrlThirdId = referUrlRuleMap.get(
							ConstantEnum.URL_THIRD_ID).trim();

					java.util.Map<ConstantEnum, String> keywordRuleMap = this.dmKeywordRuleDAO
							.getDMOjb(referUrlInfo);
					referUrlKeyword = keywordRuleMap.get(
							ConstantEnum.URL_KEYWORD).trim();
				}

				// mac地址
				String macInfoStr = WebCheckUtil.checkField(
						splitStr[MPVEnum.MAC.ordinal()], "000000000000");
				String macInfo = MACFormatUtil.getCorrectMac(macInfoStr);
				String userAgentStr = splitStr[MPVEnum.USERAGENT.ordinal()]
						.replaceAll("\\s+", "");

				java.util.Map<ConstantEnum, String> sysAndbroMap = SystemInfoUtil
						.getBroAndOSInfo(userAgentStr);
				String browseInfo = "";
				String systemInfo = "";
				if (userAgentStr != null && !"".equals(userAgentStr)) {
					browseInfo = sysAndbroMap.get(ConstantEnum.BROWSE_INFO)
							.trim();
					systemInfo = sysAndbroMap.get(ConstantEnum.SYSTEM_INFO)
							.trim();
				}
				String vTime = splitStr[MPVEnum.VTIME.ordinal()];
				if (WebCheckUtil.checkIsNum(vTime) == false) {
					vTime = "0";
				}
				String sessionId = WebCheckUtil.checkField(
						splitStr[MPVEnum.SESSIONID.ordinal()].replaceAll(
								"\\s+", ""), DEFAULT_NEGATIVE_NUM);

				String userId = splitStr[MPVEnum.USERID.ordinal()];
				if (WebCheckUtil.checkIsNum(userId) == false) {
					userId = "0";
				}

				String pvId = splitStr[MPVEnum.PVID.ordinal()];
				if (WebCheckUtil.checkIsNum(pvId) == false) {
					pvId = "0";
				}

				String pvStep = splitStr[MPVEnum.STEP.ordinal()];
				if (WebCheckUtil.checkIsNum(pvStep) == false) {
					pvStep = "0";
				}
				String pvSeStep = splitStr[MPVEnum.SESTEP.ordinal()];
				if (WebCheckUtil.checkIsNum(pvSeStep) == false) {
					pvSeStep = DEFAULT_NEGATIVE_NUM;
				}
				String seIdCount = splitStr[MPVEnum.SEIDCOUNT.ordinal()];
				if (WebCheckUtil.checkIsNum(seIdCount) == false) {
					seIdCount = "0";
				}
				String proID = splitStr[MPVEnum.PROTOCOL.ordinal()];
				if (WebCheckUtil.checkIsNum(proID) == false) {
					proID = DEFAULT_NEGATIVE_NUM;
				}
				String rproID = splitStr[MPVEnum.RPROTOCOL.ordinal()];
				if (WebCheckUtil.checkIsNum(rproID) == false) {
					rproID = DEFAULT_NEGATIVE_NUM;
				}
				mpvFormatStr.append(dateIdAndhourId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				mpvFormatStr.append(provinceId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(cityId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(ispId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				mpvFormatStr.append(platId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(qudaoId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(versionId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				mpvFormatStr.append(ipInfoStr
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(macInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				mpvFormatStr.append(fckInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(sessionId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(userId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(userTypeInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				mpvFormatStr.append(urlInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(urlFirstId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(urlSecondId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(urlThirdId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(urlKeyword
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(urlPage
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				mpvFormatStr.append(referUrlInfoStr
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(referUrlFirstId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(referUrlSecondId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(referUrlThirdId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(referUrlKeyword
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(referUrlPage
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				mpvFormatStr.append(browseInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(systemInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				mpvFormatStr.append(vTime
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(timestampInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(proID
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(rproID
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(clientflagInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append("0"
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				mpvFormatStr.append(pvId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(splitStr[MPVEnum.CONFIG.ordinal()]
						.replaceAll("\\s+", "")
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(splitStr[MPVEnum.EXT.ordinal()].replaceAll(
						"\\s+", "")
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(userAgentStr
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(pvStep
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(pvSeStep
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(seIdCount
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				mpvFormatStr.append(splitStr[MPVEnum.TA.ordinal()].replaceAll(
						"\\s+", "") + "");

			} catch (Exception e) {
				throw e;
			}

			return mpvFormatStr.toString();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String line = value.toString();
				String mpvETLStr = this.getMpvFormatStr(line);
				String[] pvStr = StringFormatUtil.splitLog(mpvETLStr,
						StringFormatUtil.TAB_SEPARATOR);
				if (pvStr.length == MPVFormatEnum.TA.ordinal() + 1) {
					keyText.set(pvStr[MPVFormatEnum.TIME_STAMP.ordinal()]);
					valueText.set(mpvETLStr);
					if (null != mpvETLStr && !("".equalsIgnoreCase(mpvETLStr))) {
						context.write(keyText, valueText);
					}
				}
			} catch (Exception e) {
				multipleOutputs.write(
						new Text(null == e.getMessage() ? ("error:" + filePath)
								: e.getMessage()), new Text(value.toString()),
						"_error/part");
				e.printStackTrace();
			}
		}
	}

	public static class MPVFormatReduce extends
			Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {

		int nRet = ToolRunner.run(new Configuration(), new MPVFormatMR(), args);
		System.out.println(nRet);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "MPVFormatMR");
		job.setJarByClass(MPVFormatMR.class);
		job.setMapperClass(MPVFormatMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MPVFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		String inputPathStr = job.getConfiguration().get(
				CommonConstant.INPUT_PATH);
		String outputPathStr = job.getConfiguration().get(
				CommonConstant.OUTPUT_PATH);
		FileInputFormat.setInputPaths(job, inputPathStr);
		FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
		int isInputLZOCompress = job.getConfiguration().getInt(
				CommonConstant.IS_INPUTFORMATLZOCOMPRESS, 1);
		if (1 == isInputLZOCompress) {
			job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
		}
		int result = job.waitForCompletion(true) ? 0 : 1;
		return result;
	}
}
