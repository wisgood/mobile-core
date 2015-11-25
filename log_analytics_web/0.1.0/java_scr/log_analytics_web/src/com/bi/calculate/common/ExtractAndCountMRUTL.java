package com.bi.calculate.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExtractAndCountMRUTL extends Configured implements Tool {

	public static class ExtractAndCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private String[] colNum = null;
		private String colID = null;
		private static final IntWritable one = new IntWritable(1);
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
						if(mediaId == "" || mediaId.isEmpty()){
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
				if (!colKey.isEmpty()) {
					context.write(new Text(colKey), one);
				}
				if (!colTotalKey.isEmpty()) {
					context.write(new Text(colTotalKey), one);
				}
			}
		}
	}

	public static class ExtractAndCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "ExtractAndCountMRUTL");
		job.getConfiguration().set("column", args[2]);
		job.getConfiguration().set("colid", args[3]);
		job.getConfiguration().set("colvalue", args[4]);
		job.setJarByClass(ExtractAndCountMRUTL.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(ExtractAndCountMapper.class);
		job.setReducerClass(ExtractAndCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(10);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ExtractAndCountParaArgs logArgs = new ExtractAndCountParaArgs();
		int nRet = 0;
		try {
			logArgs.init("ExtractAndCountMR.jar");
			logArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		}
		nRet = ToolRunner.run(new Configuration(), new ExtractAndCountMRUTL(),
				logArgs.getCountParam());
		System.out.println(nRet);
	}

}
