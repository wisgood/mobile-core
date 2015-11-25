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

import com.bi.log.play.format.PlayFormatEnum;

public class WebChlRegionCalcVVMR extends Configured implements Tool {

	public static class WebChlRegionCalcVVMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private String[] colNum = null;
		public static final String urlTypes[] = { "2", "3", "4", "5", "6", "7",
				"8", "9", "10" };

		public void setup(Context context) {
			colNum = context.getConfiguration().get("column").split(",");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String colKey = "";
			String colTotalKey = "";
			int isChannel = 0;
			StringBuilder valStr = new StringBuilder();
			String[] field = value.toString().trim().split("\t");
			for (int i = 0; i < urlTypes.length; i++) {
				if (field[PlayFormatEnum.REFER_SECOND_ID.ordinal()]
						.equals(urlTypes[i])) {
					isChannel = 1;
					break;
				}
			}
			if (isChannel == 1) {
				for (int i = 0; i < colNum.length; i++) {
					if (i == 1) {
						colTotalKey += "0" + "\t";
						colKey += field[Integer.parseInt(colNum[i])] + "\t";
					} else if (i == colNum.length - 1) {
						colTotalKey += field[Integer.parseInt(colNum[i])];
						colKey += field[Integer.parseInt(colNum[i])];
					} else {
						colTotalKey += field[Integer.parseInt(colNum[i])]
								+ "\t";
						colKey += field[Integer.parseInt(colNum[i])] + "\t";
					}
				}
				String vvMedia = field[PlayFormatEnum.MEDIA_ID.ordinal()];
				if (vvMedia.equals("") || vvMedia.isEmpty() || vvMedia == null) {
					vvMedia = "-1";
				}
				valStr.append(vvMedia + "\t");
				valStr.append(field[PlayFormatEnum.FCK.ordinal()]);

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

	public static class WebChlRegionCalcVVReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int vvCount = 0;
			String timeSpend = "0";
			HashSet<String> UniqUser = new HashSet<String>();
			HashSet<String> UniqMedia = new HashSet<String>();
			StringBuilder outVVStr = new StringBuilder();
			for (Text val : values) {
				vvCount += 1;
				String[] vvField = val.toString().split("\t");
				if (vvField.length > 1) {
					UniqMedia.add(vvField[0]);
					UniqUser.add(vvField[1]);
				}
			}
			outVVStr.append(Integer.toString(vvCount) + "\t");
			outVVStr.append(Integer.toString(UniqMedia.size()) + "\t");
			outVVStr.append(timeSpend + "\t");
			outVVStr.append(Integer.toString(UniqUser.size()));

			context.write(key, new Text(outVVStr.toString()));
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "WebChlRegionCalcVVMR");
		job.getConfiguration().set("column", args[2]);
		job.setJarByClass(WebChlRegionCalcVVMR.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(WebChlRegionCalcVVMapper.class);
		job.setReducerClass(WebChlRegionCalcVVReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		WithOneColumnParaArgs logArgs = new WithOneColumnParaArgs();
		int nRet = 0;
		try {
			logArgs.init("WebChlRegionCalcVVMR.jar");
			logArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(), new WebChlRegionCalcVVMR(),
				logArgs.getCountParam());
		System.out.println(nRet);
	}
}
