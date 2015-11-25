/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PvListFormatMR.java 
 * @Package com.bi.format 
 * @Description: 对PVformtS后的日志进行处理，增加检索页处理
 * @author wanghh
 * @date 2013-12-26
 * @input: /dw/logs/web/format/pv/2/
 * @output: /dw/logs/format/pv_list/2/
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.CommonConstant;
import com.bi.common.constant.ConstantEnum;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMURLListPageDAOImpl;
import com.bi.common.dimprocess.DMURLListTabTypeDAOImpl;
import com.bi.common.logenum.PvFormatEnum;
import com.bi.common.logenum.PvListFormatEnum;
import com.bi.common.util.PvListPageUtil;
import com.bi.common.util.StringFormatUtil;

public class PvListFormatMR extends Configured implements Tool {

	private static final String FIELD_TAB_SEPARATOR = "\t";

	private static final String FIELD_COMM_SEPARATOR = ",";

	private static final String DEFAULT_NEGATIVE_NUM = "-999";

	private static final String DEFAULT_TAB_NUM = "0";

	private static final int MAX_TAB_FIELD_LENGTH = 5;

	public static class PvListFormatMaper extends
			Mapper<LongWritable, Text, Text, Text> {

		private AbstractDMDAO<String, String> dmUrlListPageDAO = null;

		private AbstractDMDAO<String, String> dmUrlListTabTypeDAO = null;

		private MultipleOutputs<Text, Text> multipleOutputs;

		private String filePath = null;

		private Text keyText = null;

		private Text valueText = null;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			this.dmUrlListTabTypeDAO = new DMURLListTabTypeDAOImpl<String, String>();
			this.dmUrlListPageDAO = new DMURLListPageDAOImpl<String, String>();

			this.dmUrlListTabTypeDAO.parseDMObj(new File(
					ConstantEnum.DM_LIST_TYPE.name().toLowerCase()));
			this.dmUrlListPageDAO.parseDMObj(new File(
					ConstantEnum.DM_LEVEL_CN_LIST.name().toLowerCase()));

			multipleOutputs = new MultipleOutputs<Text, Text>(context);

			keyText = new Text();
			valueText = new Text();

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			filePath = fileSplit.getPath().getParent().toString();
		}

		private String formatListTab(String listPageData, String listTabs)
				throws Exception {

			if (listPageData.isEmpty() || null == listPageData) {
				return DEFAULT_NEGATIVE_NUM;
			}
			StringBuilder tabIds = new StringBuilder();
			String[] listFields = listTabs.split(FIELD_COMM_SEPARATOR, -1);
			tabIds.append(listPageData + FIELD_TAB_SEPARATOR);
			int listLengthId = Integer.parseInt(listFields[0].trim());
			tabIds.append(listFields[0].trim() + FIELD_TAB_SEPARATOR);
			int tabTypeLength = listLengthId / 10;
			int tabListLength = listFields.length - 1;

			Map<String, String> tabMap = PvListPageUtil
					.getURLListTabList(listPageData);
			if (tabMap.size() <= 0) {
				return DEFAULT_NEGATIVE_NUM;
			} else if (1 == tabMap.size()) {
				if (tabMap.containsKey(DEFAULT_NEGATIVE_NUM)) {
					return DEFAULT_NEGATIVE_NUM;
				} else if (tabMap.containsKey(DEFAULT_TAB_NUM)) {
					tabIds.append("1" + FIELD_TAB_SEPARATOR);
					for (int i = 0; i < MAX_TAB_FIELD_LENGTH - 1; i++) {
						if (i < MAX_TAB_FIELD_LENGTH - 2)
							tabIds.append(DEFAULT_TAB_NUM + FIELD_TAB_SEPARATOR);
						else
							tabIds.append(DEFAULT_TAB_NUM + "");
					}
					return tabIds.toString();
				}
			}
			if (tabMap.size() >= 1) {
				int lastTabNum = 0;
				for (int i = 0; i < tabListLength; i++) {
					String listType = listFields[i + 1].trim();
					if (i + 1 < tabTypeLength) {
						if (tabMap.containsKey(listType)) {
							String tabVal = tabMap.get(listType);
							if (i == 0) {
								tabIds.append(tabVal + FIELD_TAB_SEPARATOR);
							} else {
								String tabValuePair = listType + "-" + tabVal;
								String tabNum = dmUrlListPageDAO
										.getDMOjb(tabValuePair);
								tabIds.append(tabNum + FIELD_TAB_SEPARATOR);
							}
						} else {
							if (i == 0) {
								tabIds.append("1" + FIELD_TAB_SEPARATOR);
							} else {
								tabIds.append(DEFAULT_TAB_NUM
										+ FIELD_TAB_SEPARATOR);
							}
						}
					} else {
						if (tabMap.containsKey(listType)) {
							String tabVal = tabMap.get(listType);
							String tabValuePair = listType + "-" + tabVal;
							int tabNum = Integer.parseInt(dmUrlListPageDAO
									.getDMOjb(tabValuePair).trim());
							int cmpNum = i - tabTypeLength + 1;
							int tabId = tabNum - 20 * cmpNum;
							lastTabNum = lastTabNum | (tabId << (cmpNum * 4));
						}
					}
				}
				for(int j = 0; j < MAX_TAB_FIELD_LENGTH - tabListLength; j++){
					tabIds.append(DEFAULT_TAB_NUM + FIELD_TAB_SEPARATOR);
				}
				tabIds.append(Integer.toString(lastTabNum) + "");
			}
			return tabIds.toString();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String pvLine = value.toString();
				String[] pvFields = StringFormatUtil.splitLog(pvLine,
						StringFormatUtil.TAB_SEPARATOR);
				// String[] pvFields = .split(FIELD_TAB_SEPARATOR, -1);
				if (pvFields.length < PvFormatEnum.TA.ordinal()) {
					return;
				}
				String urlFieldStr = pvFields[PvFormatEnum.URL.ordinal()];
				String urlSecondId = pvFields[PvFormatEnum.URL_THIRD_ID
						.ordinal()];

				String urlTabList = this.dmUrlListTabTypeDAO
						.getDMOjb(urlSecondId);
				if (urlTabList.equals("-1")) {
					return;
				}
				String urlTabFields = formatListTab(urlFieldStr, urlTabList);
				if (null == urlTabFields || urlTabFields.isEmpty()
						|| urlTabFields.equals(DEFAULT_NEGATIVE_NUM)) {
					return;
				}
				// System.out.print("list change result:" + "\t" + urlTabFields
				// + "\n");

				if (!urlFieldStr.isEmpty()) {
					String pvLineNew = pvLine.replaceFirst(urlFieldStr, urlTabFields);
					 String[] pvListStr = StringFormatUtil.splitLog(pvLineNew, StringFormatUtil.TAB_SEPARATOR);
					 if (pvListStr.length == PvListFormatEnum.TA.ordinal() + 1) {
						 keyText.set(pvFields[PvFormatEnum.TIME_STAMP.ordinal()]);
						 valueText.set(pvLineNew);

						 if (null != pvLineNew && !("".equalsIgnoreCase(pvLineNew))) {
							 context.write(keyText, valueText);
						 }
					 }
				}
			} catch (Exception e) {
				multipleOutputs.write(
						new Text(null == e.getMessage() ? ("error:" + filePath)
								: e.getMessage()), new Text(value.toString()),
						"_error/part");
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			multipleOutputs.close();
		}
	}

	public static class PvListFormatReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int nRet = ToolRunner.run(new Configuration(), new PvListFormatMR(),
				args);
		System.out.println(nRet);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "PvListFormatMR");
		job.setJarByClass(PvListFormatMR.class);
		job.setMapperClass(PvListFormatMaper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PvListFormatReducer.class);
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
