/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PvFormatMR.java 
 * @Package com.bi.format 
 * @Description: 对日志名进行处理
 * @author wanghh
 * @date 2013-12-30
 * @last modifid date: 2014-1-15-11:30
 * @input: /dw/logs/web/origin/pv/3/
 * @output: /dw/logs/format/pv/2/
 * @executeCmd: hadoop jar ....
 * @inputFormat: DateId HourId ...
 * @ouputFormat: DateId MacCode ..
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.Logger;
import com.bi.common.constant.CommonConstant;
import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DefaultFieldValueEnum;
import com.bi.common.constant.UtilComstrantsEnum;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMInterURLImpl;
import com.bi.common.dimprocess.DMKeywordRuleDAOImpl;
import com.bi.common.dimprocess.DMOuterURLRuleImpl;
import com.bi.common.logenum.PvEnum;
import com.bi.common.logenum.PvFormatEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.MediaInfoUtil;
import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.common.util.StringFormatUtil;
import com.bi.common.util.SystemInfoUtil;
import com.bi.common.util.TimestampFormatNewUtil;
import com.bi.common.util.WebCheckUtil;

public class PvFormatMR extends Configured implements Tool {

	private static final String DEFAULT_NEGATIVE_NUM = "-999";

	public static class PvFormatMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private static final String urlSeparator = "http://";

		/*private static Logger logger = Logger.getLogger(PvFormatMap.class
				.getName());*/

		private String dateId = null;

		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmOuterURLRuleDAO = null;

		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmKeywordRuleDAO = null;

		private DMInterURLImpl dmInterURLRuleDAO = null;

		private Text keyText = null;

		private Text valueText = null;
		
		private MultipleOutputs<Text, Text> multipleOutputs;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);
			this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
			this.dmOuterURLRuleDAO = new DMOuterURLRuleImpl<String, Map<ConstantEnum, String>>();
			this.dmKeywordRuleDAO = new DMKeywordRuleDAOImpl<String, Map<ConstantEnum, String>>();
			this.dmInterURLRuleDAO = new DMInterURLImpl();

			this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
					.toLowerCase()));
			this.dmOuterURLRuleDAO.parseDMObj(new File(
					ConstantEnum.DM_OUTER_URL.name().toLowerCase()));
			this.dmInterURLRuleDAO.parseDMObj(new File(
					ConstantEnum.DM_INTER_URL.name().toLowerCase()));
			this.dmKeywordRuleDAO.parseDMObj(new File(
					ConstantEnum.DM_URL_KEYWORD_2.name().toLowerCase()));
			dateId = context.getConfiguration().get("dateid");
			
			multipleOutputs = new MultipleOutputs<Text, Text>(context);

			keyText = new Text();
			valueText = new Text();
		}

		public String getPVFormatStr(String originalData) throws Exception{

			StringBuilder pvFormatStr = new StringBuilder();
			String[] rawsplitStr = originalData.split("\t", -1);

			String buildData = originalData;
			if (rawsplitStr.length < PvEnum.TA.ordinal() + 1) {
				StringBuilder rawData = new StringBuilder(originalData);
				for (int i = rawsplitStr.length; i < PvEnum.TA.ordinal() + 1; i++) {
					rawData.append("" + StringFormatUtil.TAB_SEPARATOR
							+ DEFAULT_NEGATIVE_NUM);
				}
				buildData = rawData.toString();
			}
			String[] splitStr = buildData.split("\t", -1);
			
			try {

				String timestampInfo = splitStr[PvEnum.TIMESTAMP.ordinal()]
						.trim();
				String dateIdAndhourId = TimestampFormatNewUtil
						.formatTimestamp(timestampInfo, dateId);
				// String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
				// String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);

				String versionInfo = WebCheckUtil.checkField(
						splitStr[PvEnum.VERSION.ordinal()], "0.0.0.0");
				long versionId = -1;
				versionId = IPFormatUtil.ip2long(versionInfo);

				String ipInfoStr = WebCheckUtil.checkField(
						splitStr[PvEnum.IP.ordinal()], "0.0.0.0");
				long ipLong = 0;
				ipLong = IPFormatUtil.ip2long(ipInfoStr);
				java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
						.getDMOjb(ipLong);
				String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
				String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
				String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

				String fckStr = splitStr[PvEnum.FCK.ordinal()];
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
				//String clientflagInfo = "1";
				
				String clientflagInfo = WebCheckUtil.checkField(splitStr[PvEnum.CLIENTFLAG.ordinal()], "0");

				String clientflag = WebCheckUtil.checkClientFlag(clientflagInfo);

				String qudaoId = splitStr[PvEnum.QUDAO_ID.ordinal()];
				if (WebCheckUtil
						.checkIsNum(qudaoId) == false) {
					qudaoId = "0";
				}

				String urlInfoRegion = splitStr[PvEnum.URL.ordinal()];
				String urlInfo = StringDecodeFormatUtil.decodeCodedStr(
						urlInfoRegion, "utf-8").replaceAll("\\s+", "");
				if (urlInfo.isEmpty() || null == urlInfo) {
					urlInfo = DEFAULT_NEGATIVE_NUM;
				}

				java.util.Map<ConstantEnum, String> URLTypeMap = this.dmInterURLRuleDAO
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

				String referUrlInfoRegion = splitStr[PvEnum.REFERURL.ordinal()];
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
				if ((referUrl.contains(".funshion.com") && !referUrl
						.contains("vas.funshion.com"))
						|| referUrl.contains("news.smgbb.cn")) {
					URLTypeMap = this.dmInterURLRuleDAO.getDMOjb(referUrlInfoStr);
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
							.getDMOjb(referUrl);

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
						splitStr[PvEnum.MAC.ordinal()], "0");
				String macInfo = MACFormatUtil.getCorrectMac(macInfoStr);
				String userAgentStr = splitStr[PvEnum.USERAGENT.ordinal()]
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
				String vTime = splitStr[PvEnum.VTIME.ordinal()];
				if (WebCheckUtil.checkIsNum(vTime) == false) {
					vTime = DEFAULT_NEGATIVE_NUM;
				}
				String sessionId = WebCheckUtil.checkField(
						splitStr[PvEnum.SESSIONID.ordinal()].replaceAll("\\s+",
								""), DEFAULT_NEGATIVE_NUM);

				String userId = splitStr[PvEnum.USERID.ordinal()];
				if (WebCheckUtil.checkIsNum(userId) == false) {
					userId = "0";
				}

				String pvId = splitStr[PvEnum.PVID.ordinal()];
				if (WebCheckUtil.checkIsNum(pvId) == false) {
					pvId = DEFAULT_NEGATIVE_NUM;
				}

				String pvStep = splitStr[PvEnum.STEP.ordinal()];
				if (WebCheckUtil.checkIsNum(pvStep) == false) {
					pvStep = "0";
				}
				String pvSeStep = splitStr[PvEnum.SESTEP.ordinal()];
				if (WebCheckUtil.checkIsNum(pvSeStep) == false) {
					pvSeStep = DEFAULT_NEGATIVE_NUM;
				}
				String seIdCount = splitStr[PvEnum.SEIDCOUNT.ordinal()];
				if (WebCheckUtil.checkIsNum(seIdCount) == false) {
					seIdCount = DEFAULT_NEGATIVE_NUM;
				}
				String proID = splitStr[PvEnum.PROTOCOL.ordinal()];
				if (WebCheckUtil.checkIsNum(proID) == false) {
					proID = "0";
				}
				String rproID = splitStr[PvEnum.RPROTOCOL.ordinal()];
				if (WebCheckUtil.checkIsNum(rproID) == false) {
					rproID = "0";
				}
				pvFormatStr.append(dateIdAndhourId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				pvFormatStr.append(provinceId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(cityId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(ispId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				pvFormatStr.append(clientflag
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(qudaoId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(versionId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				pvFormatStr.append(ipInfoStr
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(macInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				pvFormatStr.append(fckInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(sessionId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(userId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(userTypeInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				pvFormatStr.append(urlInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(urlFirstId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(urlSecondId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(urlThirdId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(urlKeyword
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(urlPage
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				pvFormatStr.append(referUrlInfoStr
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(referUrlFirstId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(referUrlSecondId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(referUrlThirdId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(referUrlKeyword
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(referUrlPage
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				pvFormatStr.append(browseInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(systemInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				pvFormatStr.append(vTime
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(timestampInfo
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(proID
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(rproID
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(splitStr[PvEnum.FPC.ordinal()].replaceAll(
						"\\s+", "")
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				pvFormatStr.append(pvId
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(splitStr[PvEnum.CONFIG.ordinal()]
						.replaceAll("\\s+", "")
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(splitStr[PvEnum.PAGE_TYPE.ordinal()]
						.replaceAll("\\s+", "")
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(userAgentStr
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(pvStep
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(pvSeStep
						+ UtilComstrantsEnum.tabSeparator.getValueStr());
				pvFormatStr.append(seIdCount
						+ UtilComstrantsEnum.tabSeparator.getValueStr());

				pvFormatStr.append(splitStr[PvEnum.TA.ordinal()].trim() + "");

			} catch (Exception e) {
				throw e;
			}
			return pvFormatStr.toString();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			String pvETLStr;
			try {
				pvETLStr = this.getPVFormatStr(line);
				String[] pvStr = pvETLStr.split("\t", -1);
				if (pvStr.length == PvFormatEnum.TA.ordinal() + 1) {
					keyText.set(pvStr[PvFormatEnum.TIME_STAMP.ordinal()]);
					valueText.set(pvETLStr);
					if (null != pvETLStr && !("".equalsIgnoreCase(pvETLStr))) {
						context.write(keyText, valueText);
					}
				} else{
					multipleOutputs.write(new Text(pvStr[PvFormatEnum.TIME_STAMP.ordinal()]), new Text(pvETLStr),
							"_error/part");
				}
			} catch (Exception e) {
				multipleOutputs.write(
						new Text(null == e.getMessage() ? ("error:")
								: e.getMessage()), new Text(value.toString()),
						"_error/part");
				e.printStackTrace();
			}
		
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}

	public static class PvFormatReduce extends
			Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, NullWritable.get());
			}
		}
	}

	/**
	 * @throws Exception
	 * @Title: main
	 * @Description: 方法的作用
	 * @param args
	 *            参数说明
	 * @return 返回类型说明
	 * @throws
	 */
	public static void main(String[] args) throws Exception {

		int nRet = ToolRunner.run(new Configuration(), new PvFormatMR(), args);
		System.out.println(nRet);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "PvFormatMR");
		job.setJarByClass(PvFormatMR.class);
		job.setMapperClass(PvFormatMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PvFormatReduce.class);
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
		// LzoIndexer lzoIndexer = new LzoIndexer(conf);
		// lzoIndexer.index(new Path(outputPathStr));
		return result;
	}
}
