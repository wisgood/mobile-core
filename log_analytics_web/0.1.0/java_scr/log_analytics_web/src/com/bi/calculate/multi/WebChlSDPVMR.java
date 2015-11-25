package com.bi.calculate.multi;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.log.pv.format.dataenum.PvFormatEnum;

public class WebChlSDPVMR extends Configured implements Tool {

	public static class WebChlSDPVMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		// private String flagId = "0";
		private String typeId = "0";
		private String[] colNum = null;
		public static final String urlTypes[] = { "2", "3", "4", "5", "6", "7",
				"8", "9", "10" };

		public void setup(Context context) {
			colNum = context.getConfiguration().get("column").split(",");
			this.typeId = context.getConfiguration().get("type");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String colKey = "";
			String colTotalKey = "";
			int isChannel = 0;
			int typeIndex = 8;
			StringBuilder valStr = new StringBuilder();
			String[] field = value.toString().trim().split("\t");
			if (Integer.parseInt(typeId) == 0) {
				typeIndex = 8;
			} else if (Integer.parseInt(typeId) == 1) {
				typeIndex = 13;
			}
			for (int i = 0; i < urlTypes.length; i++) {
				if (field[typeIndex].equals(urlTypes[i])) {
					isChannel = 1;
					break;
				}
			}
			if (isChannel == 1) {
				for (int i = 0; i < colNum.length; i++) {
					if (i == 1) {
						colTotalKey += "0" + "\t";
						colKey += field[typeIndex - 1] + "\t";
					} /*
					 * else if (i == colNum.length - 1) { colTotalKey +=
					 * field[Integer.parseInt(colNum[i])]; colKey +=
					 * field[Integer.parseInt(colNum[i])]; }
					 */
					else if (i == 3) {
						if (Integer.parseInt(typeId) == 0) {
							if (Integer.parseInt(field[typeIndex + 6]) > 1000) {
								colTotalKey += "1" + "\t";
								colKey += "1" + "\t";
							} else {
								colTotalKey += "2" + "\t";
								colKey += "2" + "\t";
							}
						} else {
							if (Integer.parseInt(field[typeIndex - 4]) < 1000) {
								colTotalKey += "3" + "\t";
								colKey += "3" + "\t";
							}
						}
					} else if (i == 2) {
						colTotalKey += field[typeIndex] + "\t";
						colKey += field[typeIndex] + "\t";
					} else if (i == 4) {
						if (Integer.parseInt(typeId) == 0) {
							String decodedURL = StringDecodeFormatUtil
									.decodeCodedStr(
											field[PvFormatEnum.REFERURL
													.ordinal()],
											"UTF-8").replaceAll("\\s", "");
							if (Integer
									.parseInt(field[PvFormatEnum.REFER_THIRD_ID
											.ordinal()]) > 1000) {

								if (decodedURL.startsWith("http")) {
									String[] strURLLevel = decodedURL.trim()
											.split("/");
									String firstLevelStr = null;
									if (strURLLevel.length > 3) {
										if (!strURLLevel[2].isEmpty()) {
											firstLevelStr = strURLLevel[2]
													.trim();
											if (firstLevelStr.equals("")
													|| firstLevelStr == null) {
												firstLevelStr = "empty";
											}
											colTotalKey += firstLevelStr;
											colKey += firstLevelStr;
										}
									}
								} else {
									colTotalKey += decodedURL;
									colKey += decodedURL;
								}
							} else {
								if (!decodedURL.equals("")
										&& !decodedURL.isEmpty()) {
									colTotalKey += decodedURL;
									colKey += decodedURL;
								}
							}
						} else {
							String decodedURL = StringDecodeFormatUtil
									.decodeCodedStr(
											field[PvFormatEnum.URL.ordinal()],
											"UTF-8").replaceAll("\\s", "");
							if (!decodedURL.equals("") && !decodedURL.isEmpty()) {
								colTotalKey += decodedURL;
								colKey += decodedURL;
							}
						}
					} else {
						colTotalKey += field[Integer.parseInt(colNum[i])]
								+ "\t";
						colKey += field[Integer.parseInt(colNum[i])] + "\t";
					}
				}
				valStr.append(field[PvFormatEnum.SESSIONID.ordinal()] + "\t");
				valStr.append(field[PvFormatEnum.USER_FLAG.ordinal()] + "\t");
				valStr.append(field[PvFormatEnum.FCK.ordinal()]);
				if (!colKey.isEmpty()) {
					context.write(new Text(colKey), new Text(valStr.toString()));
				}
				if (!colTotalKey.isEmpty()) {
					context.write(new Text(colTotalKey),
							new Text(valStr.toString()));
				}
			}
		}
	}

	public static class WebChlSDPVReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int pvCount = 0;
			HashSet<String> UniqUser = new HashSet<String>();
			HashSet<String> UniqSession = new HashSet<String>();

			StringBuilder outPVStr = new StringBuilder();
			for (Text val : values) {
				pvCount += 1;
				String[] pvField = val.toString().split("\t");
				UniqSession.add(pvField[0]);
				UniqUser.add(pvField[2]);
			}
			outPVStr.append(Integer.toString(pvCount) + "\t");
			outPVStr.append(Integer.toString(UniqUser.size()) + "\t");
			outPVStr.append(Integer.toString(UniqSession.size()));

			context.write(key, new Text(outPVStr.toString()));
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "WebChlSDPVMR");
		job.getConfiguration().set("column", args[2]);
		job.getConfiguration().set("type", args[3]);
		job.setJarByClass(WebChlSDPVMR.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(WebChlSDPVMapper.class);
		job.setReducerClass(WebChlSDPVReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(15);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		WithOneTypeParaArgs logArgs = new WithOneTypeParaArgs();
		int nRet = 0;
		try {
			logArgs.init("WebChlSDPVMR.jar");
			logArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		}
		nRet = ToolRunner.run(new Configuration(), new WebChlSDPVMR(),
				logArgs.getCountParam());
		System.out.println(nRet);
	}
}
