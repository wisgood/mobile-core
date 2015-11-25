package com.bi.calculate.common;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

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

import com.bi.common.init.CommArgs;
import com.bi.log.play.format.PlayFormatEnum;

public class MediaNewCountMR extends Configured implements Tool {

	public static class MediaNewCountMap extends
			Mapper<LongWritable, Text, Text, Text> {
		
		public static final String urlTypes[] = { "2", "3", "4", "5", "6", "7",
			"8", "9", "10" };

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException,
				UnsupportedEncodingException {
			try {
				String line = value.toString();
				String field[] = line.split("\t");

				if (field.length == 4) {
					context.write(new Text(field[0]), new Text(
							line));
				}
				else {
					for(int i = 0; i < urlTypes.length; i++){
						if(field[PlayFormatEnum.REFER_SECOND_ID.ordinal()].equals(urlTypes[i])){
							String mediaId = field[PlayFormatEnum.MEDIA_ID.ordinal()];
							if(mediaId == "" || mediaId.isEmpty()){
								mediaId = "-1";
							}
							context.write(new Text(mediaId), new Text(line));
							break;
						}
					}
				}
			} catch (ArrayIndexOutOfBoundsException e) {

			}
		}
	}

	public static class MediaNewCountReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String newout = null;

			boolean label = false;
			boolean exLabel = false;
			for (Text val : values) {
				String value = val.toString();
				String[] field = value.split("\t");
				if (field.length == 4) {
					label = true;
				} else {
					exLabel = true;
					newout = val.toString();
				}
			}
			if (!label && exLabel) {
				String[] playInfoStrs = newout.split("\t");
				String mediaout = key.toString();

				String newHistoryMediaInfoStr = mediaout + "\t"
						+ playInfoStrs[PlayFormatEnum.REFER_SECOND_ID.ordinal()]
						+ "\t"
						+ playInfoStrs[PlayFormatEnum.REFRE_FIRST_ID.ordinal()]
						+ "\t"
						+ playInfoStrs[PlayFormatEnum.DATE_ID.ordinal()];
				
				context.write(new Text(newHistoryMediaInfoStr.trim()), new Text(""));
			}
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "MediaNewCountMR");
		//job.getConfiguration().set("column", args[2]);
		job.setJarByClass(MediaNewCountMR.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MediaNewCountMap.class);
		job.setReducerClass(MediaNewCountReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		CommArgs logArgs = new CommArgs ();
		int nRet = 0;
		try {
			logArgs.init("MediaNewCountMR.jar");
			logArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(), new MediaNewCountMR(),
				logArgs.getCommsParam());
		System.out.println(nRet);
	}

}
