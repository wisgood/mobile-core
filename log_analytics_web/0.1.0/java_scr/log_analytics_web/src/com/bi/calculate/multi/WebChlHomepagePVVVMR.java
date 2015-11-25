package com.bi.calculate.multi;

import java.io.IOException;
import java.text.DecimalFormat;
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
import com.bi.log.pv.format.dataenum.PvFormatEnum;

public class WebChlHomepagePVVVMR extends Configured implements Tool {

	public static class WebChlHomepagePVVVMap extends
			Mapper<LongWritable, Text, Text, Text> {
		private String[] colNum = null;

		public static final String pageType[] = { "3", "4", "5", "6", "7", "8",
				"19", "21", "28", "89", "90", "91", "92", "93", "94", "105",
				"107", "114", "175", "176", "177", "178", "179", "180", "191",
				"193", "200" };

		public void setup(Context context) {
			colNum = context.getConfiguration().get("column").split(",");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String colKey = "";
			String colTotalKey = "";
			StringBuilder valStr = new StringBuilder();
			int isChannel = 0;
			int urlIndex = 7;
			String[] field = value.toString().split("\t");

			if (field.length < 43) {
				urlIndex = 7;
			} else {
				urlIndex = 12;
			}
			for (int i = 0; i < pageType.length; i++) {
				if (field[urlIndex + 2].equalsIgnoreCase(pageType[i])) {
					isChannel = 1;
					break;
				}
			}
			if (isChannel == 1) {

				for (int i = 0; i < colNum.length; i++) {
					if (i == 1) {
						colTotalKey += "0" + "\t";
						colKey += field[urlIndex] + "\t";
					} else if (i == colNum.length - 1) {
						colTotalKey += field[urlIndex + 1];
						colKey += field[urlIndex + 1];
					} else {
						colTotalKey += field[Integer.parseInt(colNum[i])]
								+ "\t";
						colKey += field[Integer.parseInt(colNum[i])] + "\t";
					}
				}
				if (urlIndex == 7) {
					valStr.append("1" + "\t");
					valStr.append(field[PvFormatEnum.FCK.ordinal()]);
				} else {
					valStr.append("2" + "\t");
					valStr.append(field[PlayFormatEnum.FCK.ordinal()]);
				}
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

	public static class WebChlHomepagePVVVReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int pvCount = 0;
			int vvCount = 0;
			HashSet<String> UniqUser = new HashSet<String>();
			HashSet<String> UniqPlayUser = new HashSet<String>();
			DecimalFormat df = new DecimalFormat("0.00000000");

			StringBuilder homepageStr = new StringBuilder();

			for (Text val : values) {
				String[] valField = val.toString().trim().split("\t");
				if (valField[0].trim().equals("1")) {
					pvCount += 1;
					UniqUser.add(valField[1]);
				} else {
					vvCount += 1;
					UniqPlayUser.add(valField[1]);
				}
			}
			homepageStr.append(df.format((double) vvCount / pvCount) + "\t");
			homepageStr.append(Integer.toString(pvCount) + "\t");
			homepageStr.append(Integer.toString(UniqUser.size()) + "\t");

			homepageStr.append(Integer.toString(vvCount) + "\t");
			homepageStr.append(Integer.toString(UniqPlayUser.size()) + "\t");

			homepageStr
					.append(df.format((double) vvCount / UniqPlayUser.size()));

			context.write(key, new Text(homepageStr.toString()));
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "WebChlHomepagePVVVMR");
		job.getConfiguration().set("column", args[2]);
		job.setJarByClass(WebChlHomepagePVVVMR.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(WebChlHomepagePVVVMap.class);
		job.setReducerClass(WebChlHomepagePVVVReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(20);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		WithOneColumnParaArgs logArgs = new WithOneColumnParaArgs();
		int nRet = 0;
		try {
			logArgs.init("WebChlHomepagePVVVMR.jar");
			logArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		}
		nRet = ToolRunner.run(new Configuration(), new WebChlHomepagePVVVMR(),
				logArgs.getCountParam());
		System.out.println(nRet);
	}

}
