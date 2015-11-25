package com.bi.calculate.multi;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

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

import com.bi.calculate.common.ExtractAndCountParaArgs;
import com.bi.log.play.format.PlayFormatEnum;

public class WebChlMediaCountMR extends Configured implements Tool {

	public static class WebChlMediaCountMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private String[] colNum = null;
		private String colID = null;
		private String[] colValue = null;

		public void setup(Context context) {
			colNum = context.getConfiguration().get("column").trim().split(",");
			colID = context.getConfiguration().get("colid").trim();
			colValue = context.getConfiguration().get("colvalue").trim()
					.split(",");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String colKey = "";
			String colTotalKey = "";
			int isSelect = 0;
			String[] field = value.toString().trim().split("\t");
			for (int i = 0; i < colValue.length; i++) {
				if (field[Integer.parseInt(colID)].equals(colValue[i])) {
					isSelect = 1;
					break;
				}
			}
			if (isSelect == 1) {

				for (int i = 0; i < colNum.length; i++) {
					if (i == 1) {
						colTotalKey += "0" + "\t";
						colKey += field[Integer.parseInt(colNum[i])] + "\t";
					} else if (i == colNum.length - 1) {
						String mediaId = field[Integer.parseInt(colNum[i])];
						if (mediaId == "" || mediaId.isEmpty()) {
							mediaId = "-1";
						}
						colTotalKey += mediaId;
						colKey += mediaId;
					} else {
						colTotalKey += field[Integer.parseInt(colNum[i])]
								+ "\t";
						colKey += field[Integer.parseInt(colNum[i])] + "\t";
					}
				}
				String mediaInfo = field[PlayFormatEnum.MEDIA_ID.ordinal()];
				if (mediaInfo.equals("") || mediaInfo.isEmpty()) {
					mediaInfo = "-1";
				}
				if (!colKey.isEmpty()) {
					context.write(new Text(colKey), new Text(mediaInfo));
				}
				if (!colTotalKey.isEmpty()) {
					context.write(new Text(colTotalKey), new Text(mediaInfo));
				}
			}
		}
	}

	public static class WebChlMediaCountReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Map<String, Integer> mediaPlayMap = new HashMap<String, Integer>();
			for (Text val : values) {
				String mediaID = val.toString();
				if (mediaPlayMap.containsKey(mediaID)) {
					int staNum = mediaPlayMap.get(mediaID);
					staNum++;
					mediaPlayMap.put(mediaID, staNum);
				} else {
					mediaPlayMap.put(mediaID, 1);
				}
				sum += 1;
			}
			Iterator<Entry<String, Integer>> iter = mediaPlayMap.entrySet()
					.iterator();
			while (iter.hasNext()) {
				StringBuffer mediaOutInfo = new StringBuffer();
				DecimalFormat df = new DecimalFormat("0.00000000");
				Map.Entry<String, Integer> entry = (Entry<String, Integer>) iter
						.next();
				String mediaKey = entry.getKey();
				int mediaCount = entry.getValue();
				mediaOutInfo.append(mediaKey + "\t");
				mediaOutInfo.append(Integer.toString(mediaCount) + "\t");

				mediaOutInfo.append(df.format((double) mediaCount / sum));

				context.write(key, new Text(mediaOutInfo.toString()));
			}
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "WebChlMediaCountMR");
		job.getConfiguration().set("column", args[2]);
		job.getConfiguration().set("colid", args[3]);
		job.getConfiguration().set("colvalue", args[4]);
		job.setJarByClass(WebChlMediaCountMR.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(WebChlMediaCountMapper.class);
		job.setReducerClass(WebChlMediaCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ExtractAndCountParaArgs logArgs = new ExtractAndCountParaArgs();
		int nRet = 0;
		try {
			logArgs.init("WebChlMediaCountMR.jar");
			logArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		}
		nRet = ToolRunner.run(new Configuration(), new WebChlMediaCountMR(),
				logArgs.getCountParam());
		System.out.println(nRet);
	}

}
