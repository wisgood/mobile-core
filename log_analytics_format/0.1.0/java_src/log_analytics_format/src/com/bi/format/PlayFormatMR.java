/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayFormatMR.java 
 * @Package com.bi.format 
 * @Description: 对play日志进行处理
 * @author wanghh
 * @date 2013-12-14 
 * @input: /dw/logs/web/origin/play/1/
 * @output: /dw/logs/format/play/1/
 * @executeCmd: hadoop jar ....
 * @inputFormat: DateId HourId ...
 * @ouputFormat: DateId MacCode ..
 */

package com.bi.format;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMInterURLImpl;
import com.bi.common.dimprocess.DMKeywordRuleDAOImpl;
import com.bi.common.dimprocess.DMOuterURLRuleImpl;
import com.bi.common.dm.pojo.DMInfoHashEnum;
import com.bi.common.logenum.DMMediaTypeEnum;
import com.bi.common.logenum.PlayEnum;
import com.bi.common.logenum.PlayFormatEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.MediaInfoUtil;
import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.common.util.StringFormatUtil;
import com.bi.common.util.TimestampFormatNewUtil;
import com.bi.common.util.WebCheckUtil;

public class PlayFormatMR extends Configured implements Tool {

	private static final String DEFAULT_INFOHASH = "-999";

	private static final String DEFAULT_SERIAL_ID = "-999";

	private static final String FIELD_TAB_SEPARATOR = "\t";

	// private static final String FIELD_COMM_SEPARATOR = ",";

	private static final String DEFAULT_NEGATIVE_NUM = "-999";

	public static class PlayFormatMaper extends
			Mapper<LongWritable, Text, Text, Text> {

		/*
		 * private static Logger logger = Logger.getLogger(PlayFormatMaper.class
		 * .getName());
		 */
		private static final String urlSeparator = "http://";

		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmOuterURLRuleDAO = null;

		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmKeywordRuleDAO = null;

		private DMInterURLImpl dmInterURLRuleDAO = null;

		private MultipleOutputs<Text, Text> multipleOutputs;

		private String filePath = null;

		private String dateId = null;

		private Text keyText = null;

		private Text valueText = null;

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

			multipleOutputs = new MultipleOutputs<Text, Text>(context);

			dateId = context.getConfiguration().get("dateid");

			keyText = new Text();
			valueText = new Text();

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			filePath = fileSplit.getPath().getParent().toString();
		}

		public String getPlayFormatStr(String originalData) throws Exception {

			StringBuilder playFormatStr = new StringBuilder();
			String[] rawsplitStr = StringFormatUtil.splitLog(originalData,
					StringFormatUtil.TAB_SEPARATOR);

			String buildData = originalData;
			if (rawsplitStr.length < PlayEnum.SEIDCOUNT.ordinal() + 1) {
				StringBuilder rawData = new StringBuilder(originalData);
				for (int i = rawsplitStr.length; i < PlayEnum.SEIDCOUNT
						.ordinal() + 1; i++) {
					rawData.append("" + StringFormatUtil.TAB_SEPARATOR
							+ DEFAULT_NEGATIVE_NUM);
				}
				buildData = rawData.toString();
			}
			String[] splitStr = StringFormatUtil.splitLog(buildData,
					StringFormatUtil.TAB_SEPARATOR);
			try {
				// TimeStamp format, Get date_id,hour_id
				String timestampInfo = splitStr[PlayEnum.TIMESTAMP.ordinal()];
				String dateIdAndhourId = TimestampFormatNewUtil
						.formatTimestamp(timestampInfo, dateId);

				// Version
				String versionInfo = WebCheckUtil.checkField(
						splitStr[PlayEnum.VERSION.ordinal()], "0.0.0.0");
				long versionId = -1;
				versionId = IPFormatUtil.ip2long(versionInfo);
				// Ip
				String ipInfoStr = WebCheckUtil.checkField(
						splitStr[PlayEnum.IP.ordinal()], "0.0.0.0");
				long ipLong = 0;
				ipLong = IPFormatUtil.ip2long(ipInfoStr);
				Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
						.getDMOjb(ipLong);
				String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
				String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
				String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
				// Mac
				String macInfoStr = WebCheckUtil.checkField(
						splitStr[PlayEnum.MAC.ordinal()], "0");
				String macInfo = MACFormatUtil.getCorrectMac(macInfoStr);
				// Fck
				String fckStr = splitStr[PlayEnum.FCK.ordinal()];
				String fckInfo = DefaultFieldValueEnum.fckDefault.getValueStr();
				if (WebCheckUtil.checkFck(fckStr) == true) {
					fckInfo = fckStr;
				}
				String userTypeInfo = "0";
				if (MediaInfoUtil.getUserType(fckStr, dateId) == true) {
					userTypeInfo = "1";
				}
				String clientflagInfo = WebCheckUtil.checkField(
						splitStr[PlayEnum.CLIENT_FLAG.ordinal()], "1");

				String platInfo = WebCheckUtil.checkClientFlag(clientflagInfo);

				String qudaoId = splitStr[PlayEnum.ISP_ID.ordinal()];
				if (WebCheckUtil
						.checkIsNum(qudaoId) == false) {
					qudaoId = "0";
				}
				// URL
				String urlInfoRegion = WebCheckUtil.checkField(
						splitStr[PlayEnum.URL.ordinal()], DEFAULT_NEGATIVE_NUM);
				String urlInfoStr = StringDecodeFormatUtil.decodeCodedStr(
						urlInfoRegion, "utf-8");
				String urlInfo = urlInfoStr.replaceAll("\\s+", "");

				Map<ConstantEnum, String> URLTypeMap = this.dmInterURLRuleDAO
						.getDMOjb(urlInfo);
				String urlFirstId = URLTypeMap.get(ConstantEnum.URL_FIRST_ID)
						.trim();
				String urlSecondId = URLTypeMap.get(ConstantEnum.URL_SECOND_ID)
						.trim();
				String urlThirdId = URLTypeMap.get(ConstantEnum.URL_THIRD_ID)
						.trim();
				// ReferURL
				String referUrlInfoRegion = StringDecodeFormatUtil
						.decodeCodedStr(splitStr[PlayEnum.REFERURL.ordinal()],
								"utf-8");
				String referUrlInfoStr = referUrlInfoRegion.replaceAll("\\s+",
						"");
				// if (!referUrlInfoStr.startsWith("http")) {
				// referUrlInfoStr = DEFAULT_NEGATIVE_NUM

				String referUrlInfo = referUrlInfoStr;
				String referUrlFirstId = DEFAULT_NEGATIVE_NUM;
				String referUrlSecondId = DEFAULT_NEGATIVE_NUM;
				String referUrlThirdId = DEFAULT_NEGATIVE_NUM;
				String referUrlKeyword = "";
				String referUrlPage = DEFAULT_NEGATIVE_NUM;
				if (referUrlInfo.contains(urlSeparator)) {
					String referUrlInfoArr[] = referUrlInfoRegion
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
					URLTypeMap = this.dmInterURLRuleDAO
							.getDMOjb(referUrlInfoStr);
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
					Map<ConstantEnum, String> referUrlRuleMap = this.dmOuterURLRuleDAO
							.getDMOjb(referUrlInfo);
					referUrlFirstId = referUrlRuleMap.get(
							ConstantEnum.URL_FIRST_ID).trim();
					referUrlSecondId = referUrlRuleMap.get(
							ConstantEnum.URL_SECOND_ID).trim();
					referUrlThirdId = referUrlRuleMap.get(
							ConstantEnum.URL_THIRD_ID).trim();
					Map<ConstantEnum, String> keywordRuleMap = this.dmKeywordRuleDAO
							.getDMOjb(referUrlInfo);
					referUrlKeyword = keywordRuleMap.get(
							ConstantEnum.URL_KEYWORD).trim();
				}
				// MediaType
				String mediaTypeInfo = DEFAULT_NEGATIVE_NUM;
				String playMediaType = WebCheckUtil.checkField(
						splitStr[PlayEnum.MEDIATYPE.ordinal()], "0");
				Map<String, String> mediaInfoMap = MediaInfoUtil
						.getMediaType(playMediaType);
				if (mediaInfoMap != null) {
					mediaTypeInfo = mediaInfoMap.get(DMMediaTypeEnum.PLAY_TYPE
							.name());
					if (mediaTypeInfo.contains(DEFAULT_NEGATIVE_NUM)) {
						if (urlInfoStr.contains("subject/special")
								|| urlInfoStr.contains("video/play")) {
							mediaTypeInfo = "2";
						} else if (urlInfoStr.contains("subject")) {
							mediaTypeInfo = "1";
						}
					}
				}
				String mediaIDInfo = DEFAULT_NEGATIVE_NUM;
				String serialIDInfo = DEFAULT_NEGATIVE_NUM;
				Map<String, String> mediaIDMap = MediaInfoUtil
						.getMediaID(splitStr[PlayEnum.TARGET.ordinal()]);
				if (mediaIDMap != null) {
					mediaIDInfo = mediaIDMap.get(
							DMMediaTypeEnum.MEDIA_ID.name()).trim();
					serialIDInfo = mediaIDMap.get(DMMediaTypeEnum.SEARIA_ID
							.name());
				}
				if (!mediaIDInfo.contains(DEFAULT_NEGATIVE_NUM)
						&& !mediaIDInfo.isEmpty() && !mediaTypeInfo.equals("3")) {
					int mediaNum = Integer.parseInt(mediaIDInfo);
					if (mediaNum > 1000000) {
						mediaTypeInfo = "2";
					} else if (mediaNum > 0 && mediaNum < 1000000) {
						mediaTypeInfo = "1";
					}
				}
				String sessionId = WebCheckUtil.checkField(
						splitStr[PlayEnum.SESSION_ID.ordinal()], "0");
				String infohashId = WebCheckUtil.checkField(
						splitStr[PlayEnum.HASHID.ordinal()],
						DEFAULT_NEGATIVE_NUM);
				String fpcId = WebCheckUtil.checkField(
						splitStr[PlayEnum.FPC.ordinal()], "0");
				String configId = WebCheckUtil.checkField(
						splitStr[PlayEnum.CONFIG.ordinal()], "0");
				String fmtId = WebCheckUtil.checkField(
						splitStr[PlayEnum.FMT.ordinal()], "0");
				String pagetypeId = WebCheckUtil.checkField(
						splitStr[PlayEnum.PAGETYPE.ordinal()],
						DEFAULT_NEGATIVE_NUM);
				String userId = WebCheckUtil.getFieldNum(
						splitStr[PlayEnum.USERID.ordinal()], "0");
				String pvId = WebCheckUtil
						.getFieldNum(splitStr[PlayEnum.PVID.ordinal()],
								DEFAULT_NEGATIVE_NUM);
				String vvId = WebCheckUtil
						.getFieldNum(splitStr[PlayEnum.VVID.ordinal()],
								DEFAULT_NEGATIVE_NUM);
				String lianId = WebCheckUtil
						.getFieldNum(splitStr[PlayEnum.LIAN.ordinal()],
								DEFAULT_NEGATIVE_NUM);
				String proId = WebCheckUtil.getFieldNum(
						splitStr[PlayEnum.PROTOCOL.ordinal()], "0");

				String reproId = WebCheckUtil.getFieldNum(
						splitStr[PlayEnum.RPROTOCOL.ordinal()], "0");

				String platformInfo = splitStr[PlayEnum.PLATFORM.ordinal()];
				if (platformInfo.isEmpty() || null == platformInfo) {
					platformInfo = "funshion";
				}
				String videoLength = WebCheckUtil.getFieldNum(
						splitStr[PlayEnum.VIDEOLENGTH.ordinal()], "0");
				String pvStep = WebCheckUtil.getFieldNum(
						splitStr[PlayEnum.STEP.ordinal()], "0");
				String pvSeStep = WebCheckUtil.getFieldNum(
						splitStr[PlayEnum.SESTEP.ordinal()], "0");
				String seIdCount = WebCheckUtil.getFieldNum(
						splitStr[PlayEnum.SEIDCOUNT.ordinal()], "0");

				playFormatStr.append(dateIdAndhourId + FIELD_TAB_SEPARATOR);
				// playFormatStr.append(hourId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(provinceId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(cityId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(ispId + FIELD_TAB_SEPARATOR);

				playFormatStr.append(platInfo + FIELD_TAB_SEPARATOR);
				playFormatStr.append(qudaoId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(versionId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(ipInfoStr + FIELD_TAB_SEPARATOR);
				playFormatStr.append(macInfo + FIELD_TAB_SEPARATOR);
				playFormatStr.append(fckInfo + FIELD_TAB_SEPARATOR);

				playFormatStr.append(sessionId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(userId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(userTypeInfo + FIELD_TAB_SEPARATOR);

				playFormatStr.append(infohashId.toUpperCase()
						+ FIELD_TAB_SEPARATOR);
				playFormatStr.append(serialIDInfo + FIELD_TAB_SEPARATOR);
				playFormatStr.append(mediaIDInfo + FIELD_TAB_SEPARATOR);
				playFormatStr
						.append(DEFAULT_NEGATIVE_NUM + FIELD_TAB_SEPARATOR);
				playFormatStr.append("Unknown" + FIELD_TAB_SEPARATOR);
				playFormatStr.append(mediaTypeInfo + FIELD_TAB_SEPARATOR);

				playFormatStr.append(urlInfo + FIELD_TAB_SEPARATOR);
				playFormatStr.append(urlFirstId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(urlSecondId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(urlThirdId + FIELD_TAB_SEPARATOR);

				playFormatStr.append(referUrlInfoStr + FIELD_TAB_SEPARATOR);
				playFormatStr.append(referUrlFirstId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(referUrlSecondId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(referUrlThirdId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(referUrlKeyword + FIELD_TAB_SEPARATOR);
				playFormatStr.append(referUrlPage + FIELD_TAB_SEPARATOR);
				playFormatStr.append(timestampInfo + FIELD_TAB_SEPARATOR);
				playFormatStr.append(proId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(reproId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(clientflagInfo + FIELD_TAB_SEPARATOR);
				playFormatStr.append(fpcId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(pvId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(configId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(vvId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(lianId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(platformInfo + FIELD_TAB_SEPARATOR);
				playFormatStr.append(videoLength + FIELD_TAB_SEPARATOR);

				playFormatStr.append(fmtId + FIELD_TAB_SEPARATOR);

				playFormatStr.append(pagetypeId + FIELD_TAB_SEPARATOR);
				playFormatStr.append(pvStep + FIELD_TAB_SEPARATOR);
				playFormatStr.append(pvSeStep + FIELD_TAB_SEPARATOR);
				playFormatStr.append(seIdCount + "");

			} catch (Exception e) {
				// logger.error(e.getMessage(), e.getCause());
				throw e;
			}

			return playFormatStr.toString();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String line = value.toString();
				String[] fields = line.split(FIELD_TAB_SEPARATOR, -1);

				if (filePath.toLowerCase().contains("play")
						&& fields.length > PlayFormatEnum.MEDIA_TYPE_ID
								.ordinal()) {
					String playETLStr = getPlayFormatStr(line);

					String[] playField = playETLStr.split(FIELD_TAB_SEPARATOR,
							-1);
					String infohashStr = null;
					if (playField[PlayFormatEnum.MEDIA_TYPE_ID.ordinal()]
							.trim().equals("1")
							|| playField[PlayFormatEnum.URL.ordinal()]
									.contains("subject/play")) {

						infohashStr = playField[PlayFormatEnum.INFOHASH_ID
								.ordinal()].toUpperCase();
					} else {
						infohashStr = playField[PlayFormatEnum.MEDIA_ID
								.ordinal()];
					}
					if (null != infohashStr
							&& playField.length == PlayFormatEnum.SEIDCOUNT
									.ordinal() + 1) {
						keyText.set(infohashStr.trim());
						valueText.set(playETLStr);
						context.write(keyText, valueText);
					}

				} else {
					String dimLine = "";
					String dimInfo = null;
					if (filePath.toLowerCase().contains("infohash")) {
						if (fields.length > DMInfoHashEnum.MEDIA_ID.ordinal()) {
							dimLine = line.trim();
							dimInfo = fields[DMInfoHashEnum.IH.ordinal()];
						}
					} else if (filePath.toLowerCase().contains("mediainfo")) {
						StringBuilder dimStrSb = new StringBuilder();
						dimStrSb.append(DEFAULT_INFOHASH + FIELD_TAB_SEPARATOR);
						dimStrSb.append(DEFAULT_SERIAL_ID + FIELD_TAB_SEPARATOR);
						dimStrSb.append(line.trim());
						dimLine = dimStrSb.toString();
						dimInfo = fields[DMInfoHashEnum.IH.ordinal()];
					}
					if (null != dimInfo && !dimInfo.isEmpty()) {
						String mediaInfo = dimInfo.trim().toUpperCase();
						keyText.set(mediaInfo);
						valueText.set(dimLine);
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

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			multipleOutputs.close();
		}
	}

	public static class PlayFormatReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		Text keyText = null;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			keyText = new Text();
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String dimStr = null;
			List<String> playInfoList = new ArrayList<String>();
			for (Text val : values) {
				String value = val.toString();
				String[] valueField = value.split(FIELD_TAB_SEPARATOR, -1);
				if (valueField.length == DMInfoHashEnum.MEDIA_NAME.ordinal() + 1) {
					dimStr = value;
				} else {
					playInfoList.add(value);
				}
			}
			for (String playInfoString : playInfoList) {
				String playFormatValue = "";

				String[] splitMediaPlaySts = playInfoString.split(
						FIELD_TAB_SEPARATOR, -1);
				List<String> splitMediaPlayList = new ArrayList<String>();
				for (String splitMediaPlay : splitMediaPlaySts) {
					splitMediaPlayList.add(splitMediaPlay);
				}
				if (null != dimStr) {
					String[] dimStrs = dimStr.split(FIELD_TAB_SEPARATOR, -1);

					if (dimStrs.length > DMInfoHashEnum.MEDIA_NAME.ordinal()) {
						splitMediaPlayList.set(
								PlayFormatEnum.SERIAL_ID.ordinal(),
								dimStrs[DMInfoHashEnum.SERIAL_ID.ordinal()]);
						splitMediaPlayList.set(
								PlayFormatEnum.MEDIA_ID.ordinal(),
								dimStrs[DMInfoHashEnum.MEDIA_ID.ordinal()]);
						splitMediaPlayList.set(
								PlayFormatEnum.CHANNEL_ID.ordinal(),
								dimStrs[DMInfoHashEnum.CHANNEL_ID.ordinal()]);
						splitMediaPlayList.set(
								PlayFormatEnum.MEDIA_NAME.ordinal(),
								dimStrs[DMInfoHashEnum.MEDIA_NAME.ordinal()]);
					} else {
						splitMediaPlayList.set(
								PlayFormatEnum.SERIAL_ID.ordinal(),
								DEFAULT_NEGATIVE_NUM);
						splitMediaPlayList.set(
								PlayFormatEnum.MEDIA_ID.ordinal(),
								DEFAULT_NEGATIVE_NUM);
					}
				} else {
					if (splitMediaPlayList.size() > PlayFormatEnum.SERIAL_ID
							.ordinal() + 1) {
						splitMediaPlayList.set(
								PlayFormatEnum.SERIAL_ID.ordinal(),
								DEFAULT_NEGATIVE_NUM);
						splitMediaPlayList.set(
								PlayFormatEnum.MEDIA_ID.ordinal(),
								DEFAULT_NEGATIVE_NUM);
					}
				}

				for (int i = 0; i < splitMediaPlayList.size(); i++) {
					playFormatValue += splitMediaPlayList.get(i).replaceAll(
							"\\s+", "");
					if (i < splitMediaPlayList.size() - 1) {
						playFormatValue += FIELD_TAB_SEPARATOR;
					}
				}
				keyText.set(playFormatValue);
				context.write(keyText, NullWritable.get());
			}
		}
	}

	/**
	 * @throws Exception
	 * @Title: main
	 * @Description:
	 * @param 参数说明
	 * @return 返回类型说明
	 * @throws
	 */
	public static void main(String[] args) throws Exception {
		int nRet = ToolRunner
				.run(new Configuration(), new PlayFormatMR(), args);
		System.out.println(nRet);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "PlayFormatMR");
		job.setJarByClass(PlayFormatMR.class);
		job.setMapperClass(PlayFormatMaper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PlayFormatReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		String inputPathStr = job.getConfiguration().get(
				CommonConstant.INPUT_PATH);
		String outputPathStr = job.getConfiguration().get(
				CommonConstant.OUTPUT_PATH);
		FileInputFormat.setInputPaths(job, inputPathStr);
		FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
		int isInputLZOCompress = job.getConfiguration().getInt(
				CommonConstant.IS_INPUTFORMATLZOCOMPRESS, 0);
		if (1 == isInputLZOCompress) {
			job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
		}
		int result = job.waitForCompletion(true) ? 0 : 1;
		return result;
	}
}
