/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: MPlayFormatMR.java 
 * @Package com.bi.format 
 * @Description: 对mplay日志名进行处理
 * @author: wanghh
 * @date: 2014-01-16
 * @last modifid date: 2014-1-16-16:30
 * @input: /dw/logs/web/origin/mplay/1
 * @output: /dw/logs/format/mplay/1/
 * @executeCmd: hadoop jar ....
 * @inputFormat: time.......
 * @ouputFormat: DATE_ID,HOUR_ID, PROVINCE_ID,CITY_ID, ISP_ID, PLAT_ID, QUDAO_ID, VERSION_ID, IP, ..
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
import com.bi.common.dimprocess.DMKeywordRuleDAOImpl;
import com.bi.common.dimprocess.DMMobileWebURLImpl;
import com.bi.common.dimprocess.DMOuterURLRuleImpl;
import com.bi.common.dm.pojo.DMInfoHashEnum;
import com.bi.common.logenum.DMMediaTypeEnum;
import com.bi.common.logenum.MPlayEnum;
import com.bi.common.logenum.MPlayFormatEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.MediaInfoUtil;
import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.common.util.StringFormatUtil;
import com.bi.common.util.TimestampFormatNewUtil;
import com.bi.common.util.WebCheckUtil;

public class MPlayFormatMR extends Configured implements Tool {

	private static final String DEFAULT_INFOHASH = "-999";

	private static final String DEFAULT_SERIAL_ID = "-999";

	private static final String FIELD_TAB_SEPARATOR = "\t";

	// private static final String FIELD_COMM_SEPARATOR = ",";

	private static final String DEFAULT_NEGATIVE_NUM = "-999";

	public static class MPlayFormatMaper extends
			Mapper<LongWritable, Text, Text, Text> {

		private static final String urlSeparator = "http://";

		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmOuterURLRuleDAO = null;

		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmKeywordRuleDAO = null;

		private AbstractDMDAO<String, Map<ConstantEnum, String>> dmMobileWebURLDAO = null;

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

			this.dmMobileWebURLDAO = new DMMobileWebURLImpl<String, Map<ConstantEnum, String>>();

			this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
					.toLowerCase()));
			this.dmOuterURLRuleDAO.parseDMObj(new File(
					ConstantEnum.DM_OUTER_URL.name().toLowerCase()));
			this.dmMobileWebURLDAO.parseDMObj(new File(
					ConstantEnum.DM_MOBILE_WEB_INTER_URL.name().toLowerCase()));
			this.dmKeywordRuleDAO.parseDMObj(new File(
					ConstantEnum.DM_URL_KEYWORD_2.name().toLowerCase()));

			multipleOutputs = new MultipleOutputs<Text, Text>(context);

			dateId = context.getConfiguration().get("dateid");

			keyText = new Text();
			valueText = new Text();

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			filePath = fileSplit.getPath().getParent().toString();
		}

		public String getmplayFormatStr(String originalData) throws Exception {

			StringBuilder mplayFormatStr = new StringBuilder();
			String[] rawsplitStr = StringFormatUtil.splitLog(originalData,
					StringFormatUtil.TAB_SEPARATOR);

			String buildData = originalData;
			if (rawsplitStr.length < MPlayEnum.URERAGENT.ordinal() + 1) {
				StringBuilder rawData = new StringBuilder(originalData);
				for (int i = rawsplitStr.length; i < MPlayEnum.URERAGENT
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
				String timestampInfo = splitStr[MPlayEnum.TIMESTAMP.ordinal()];
				String dateIdAndhourId = TimestampFormatNewUtil
						.formatTimestamp(timestampInfo, dateId);

				// Version
				String versionInfo = WebCheckUtil.checkField(
						splitStr[MPlayEnum.VERSION.ordinal()], "0.0.0.0");
				long versionId = -1;
				versionId = IPFormatUtil.ip2long(versionInfo);
				// Ip
				String ipInfoStr = WebCheckUtil.checkField(
						splitStr[MPlayEnum.IP.ordinal()], "0.0.0.0");
				long ipLong = 0;
				ipLong = IPFormatUtil.ip2long(ipInfoStr);
				Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
						.getDMOjb(ipLong);
				String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
				String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
				String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
				// Mac
				String macInfoStr = WebCheckUtil.checkField(
						splitStr[MPlayEnum.MAC.ordinal()], "0");
				String macInfo = MACFormatUtil.getCorrectMac(macInfoStr);
				// Fck
				String fckStr = splitStr[MPlayEnum.FCK.ordinal()];
				String fckInfo = DefaultFieldValueEnum.fckDefault.getValueStr();
				if (WebCheckUtil.checkFck(fckStr) == true) {
					fckInfo = fckStr;
				}
				String userTypeInfo = "0";
				if (MediaInfoUtil.getUserType(fckStr, dateId) == true) {
					userTypeInfo = "1";
				}
				String clientflagInfo = WebCheckUtil.checkField(
						splitStr[MPlayEnum.CLIENT_FLAG.ordinal()], "4");

				String platInfo = "11";

				String qudaoId = splitStr[MPlayEnum.QUDAO_ID.ordinal()];
				if (WebCheckUtil.checkIsNum(splitStr[MPlayEnum.QUDAO_ID
						.ordinal()]) == false) {
					qudaoId = "0";
				}
				// URL
				String urlInfoRegion = WebCheckUtil
						.checkField(splitStr[MPlayEnum.URL.ordinal()],
								DEFAULT_NEGATIVE_NUM);
				String urlInfoStr = StringDecodeFormatUtil.decodeCodedStr(
						urlInfoRegion, "utf-8");
				String urlInfo = urlInfoStr.replaceAll("\\s+", "");
				if (urlInfo.isEmpty()) {
					urlInfo = DEFAULT_NEGATIVE_NUM;
				}
				Map<ConstantEnum, String> URLTypeMap = this.dmMobileWebURLDAO
						.getDMOjb(urlInfo);
				String urlFirstId = URLTypeMap.get(ConstantEnum.URL_FIRST_ID)
						.trim();
				String urlSecondId = URLTypeMap.get(ConstantEnum.URL_SECOND_ID)
						.trim();
				String urlThirdId = URLTypeMap.get(ConstantEnum.URL_THIRD_ID)
						.trim();
				// ReferURL
				String referUrlInfoRegion = StringDecodeFormatUtil
						.decodeCodedStr(splitStr[MPlayEnum.REFERURL.ordinal()],
								"utf-8");
				String referUrlInfoStr = referUrlInfoRegion.replaceAll("\\s+",
						"");

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
				if (referUrl.contains(".funshion.com")) {
					URLTypeMap = this.dmMobileWebURLDAO
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
				String configInfo = splitStr[MPlayEnum.CONFIG.ordinal()];

				String mediaTypeInfo = DEFAULT_NEGATIVE_NUM;
				String playMediaType = WebCheckUtil.checkField(
						splitStr[MPlayEnum.MEDIATYPE.ordinal()], "0");
				Map<String, String> mediaInfoMap = MediaInfoUtil
						.getMediaType(playMediaType);
				if (mediaInfoMap != null) {
					mediaTypeInfo = mediaInfoMap.get(DMMediaTypeEnum.PLAY_TYPE
							.name());
					if (mediaTypeInfo.contains(DEFAULT_NEGATIVE_NUM)) {
						if (configInfo.contains("play")
								|| configInfo.contains("subject")) {
							mediaTypeInfo = "1";
						} else {
							mediaTypeInfo = "2";
						}
					}
				}
				String mediaIDInfo = DEFAULT_NEGATIVE_NUM;
				String serialIDInfo = DEFAULT_NEGATIVE_NUM;
				Map<String, String> mediaIDMap = MediaInfoUtil
						.getMediaID(splitStr[MPlayEnum.TARGET.ordinal()]);
				if (mediaIDMap != null) {
					mediaIDInfo = mediaIDMap.get(
							DMMediaTypeEnum.MEDIA_ID.name()).trim();
					serialIDInfo = mediaIDMap.get(DMMediaTypeEnum.SEARIA_ID
							.name());
				}
				if (mediaIDInfo.isEmpty()
						|| mediaIDInfo.equals(DEFAULT_NEGATIVE_NUM)
						|| mediaIDInfo.equals("0")) {
					Map<ConstantEnum, String> mediaAbout = MediaInfoUtil
							.getMobileWebMediaInfo(urlInfo);
					if (mediaAbout.size() > 0) {
						mediaIDInfo = mediaAbout.get(ConstantEnum.MEIDA_ID);
					}
				}

				String sessionId = WebCheckUtil.checkField(
						splitStr[MPlayEnum.SESSION_ID.ordinal()], "0");
				String infohashId = WebCheckUtil.checkField(
						splitStr[MPlayEnum.HASHID.ordinal()],
						DEFAULT_NEGATIVE_NUM);
				String fpcId = "0";
				String configId = WebCheckUtil.checkField(configInfo, "0");
				String fmtId = WebCheckUtil.checkField(
						splitStr[MPlayEnum.FMT.ordinal()], "0");
				String pagetypeId = WebCheckUtil
						.checkField(splitStr[MPlayEnum.EXT.ordinal()],
								DEFAULT_NEGATIVE_NUM);
				String userId = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.USERID.ordinal()], "0");
				String pvId = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.PVID.ordinal()], "0");
				String vvId = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.VVID.ordinal()], "0");
				String lianId = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.LIAN.ordinal()],
						DEFAULT_NEGATIVE_NUM);
				String proId = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.PROTOCOL.ordinal()], "0");

				String reproId = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.RPROTOCOL.ordinal()], "0");

				String platformInfo = splitStr[MPlayEnum.PLATFORM.ordinal()];
				if (platformInfo.isEmpty() || null == platformInfo) {
					platformInfo = "funshion";
				}
				String videoLength = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.VIDEOLENGTH.ordinal()], "0");
				String pvStep = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.STEP.ordinal()], "0");
				String pvSeStep = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.SESTEP.ordinal()], "0");
				String seIdCount = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.SEIDCOUNT.ordinal()], "0");
				String userAgent = WebCheckUtil.getFieldNum(
						splitStr[MPlayEnum.URERAGENT.ordinal()], "0")
						.replaceAll("\\s+", "");
				mplayFormatStr.append(dateIdAndhourId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(provinceId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(cityId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(ispId + FIELD_TAB_SEPARATOR);

				mplayFormatStr.append(platInfo + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(qudaoId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(versionId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(ipInfoStr + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(macInfo + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(fckInfo + FIELD_TAB_SEPARATOR);

				mplayFormatStr.append(sessionId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(userId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(userTypeInfo + FIELD_TAB_SEPARATOR);

				mplayFormatStr.append(infohashId.toUpperCase()
						+ FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(serialIDInfo + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(mediaIDInfo + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(DEFAULT_NEGATIVE_NUM
						+ FIELD_TAB_SEPARATOR);
				mplayFormatStr.append("Unknown" + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(mediaTypeInfo + FIELD_TAB_SEPARATOR);

				mplayFormatStr.append(urlInfo + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(urlFirstId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(urlSecondId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(urlThirdId + FIELD_TAB_SEPARATOR);

				mplayFormatStr.append(referUrlInfoStr + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(referUrlFirstId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(referUrlSecondId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(referUrlThirdId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(referUrlKeyword + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(referUrlPage + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(timestampInfo + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(proId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(reproId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(clientflagInfo + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(fpcId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(pvId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(configId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(vvId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(lianId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(platformInfo + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(videoLength + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(fmtId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(pagetypeId + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(pvStep + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(pvSeStep + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(seIdCount + FIELD_TAB_SEPARATOR);
				mplayFormatStr.append(userAgent + "");

			} catch (Exception e) {
				throw e;
			}

			return mplayFormatStr.toString();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String line = value.toString();
				String[] fields = line.split(FIELD_TAB_SEPARATOR, -1);

				if (filePath.toLowerCase().contains("play")
						&& fields.length > MPlayFormatEnum.MEDIA_TYPE_ID
								.ordinal()) {

					String playETLStr = getmplayFormatStr(line);

					String[] playField = playETLStr.split(FIELD_TAB_SEPARATOR,
							-1);
					String infohashStr = null;
					
					if (playField[MPlayFormatEnum.MEDIA_TYPE_ID.ordinal()]
							.trim().equals("1")
							|| playField[MPlayFormatEnum.URL.ordinal()]
									.contains("play")
							|| playField[MPlayFormatEnum.URL.ordinal()]
									.contains("subject")) {

						infohashStr = playField[MPlayFormatEnum.INFOHASH_ID
								.ordinal()].toUpperCase();
					} else {
						infohashStr = playField[MPlayFormatEnum.MEDIA_ID
								.ordinal()];
					}
					if (null != infohashStr
							&& playField.length == MPlayFormatEnum.USERAGENT
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

	public static class MPlayFormatReducer extends
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
								MPlayFormatEnum.SERIAL_ID.ordinal(),
								dimStrs[DMInfoHashEnum.SERIAL_ID.ordinal()]);
						splitMediaPlayList.set(
								MPlayFormatEnum.MEDIA_ID.ordinal(),
								dimStrs[DMInfoHashEnum.MEDIA_ID.ordinal()]);
						splitMediaPlayList.set(
								MPlayFormatEnum.CHANNEL_ID.ordinal(),
								dimStrs[DMInfoHashEnum.CHANNEL_ID.ordinal()]);
						splitMediaPlayList.set(
								MPlayFormatEnum.MEDIA_NAME.ordinal(),
								dimStrs[DMInfoHashEnum.MEDIA_NAME.ordinal()]);
					} else {
						splitMediaPlayList.set(
								MPlayFormatEnum.SERIAL_ID.ordinal(),
								DEFAULT_NEGATIVE_NUM);
						splitMediaPlayList.set(
								MPlayFormatEnum.MEDIA_ID.ordinal(),
								DEFAULT_NEGATIVE_NUM);
					}
				} else {
					if (splitMediaPlayList.size() > MPlayFormatEnum.SERIAL_ID
							.ordinal() + 1) {
						splitMediaPlayList.set(
								MPlayFormatEnum.SERIAL_ID.ordinal(),
								DEFAULT_NEGATIVE_NUM);
						splitMediaPlayList.set(
								MPlayFormatEnum.MEDIA_ID.ordinal(),
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
		int nRet = ToolRunner.run(new Configuration(), new MPlayFormatMR(),
				args);
		System.out.println(nRet);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "MPlayFormatMR");
		job.setJarByClass(MPlayFormatMR.class);
		job.setMapperClass(MPlayFormatMaper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MPlayFormatReducer.class);
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
