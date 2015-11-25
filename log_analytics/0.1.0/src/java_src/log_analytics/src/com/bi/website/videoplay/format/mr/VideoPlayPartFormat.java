package com.bi.website.videoplay.format.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.website.mediaplay.format.MediaPlayInfohashJoinMR;
import com.bi.website.mediaplay.format.MediaPlayJoinArgs;
import com.bi.website.mediaplay.format.MediaPlayJoinFormat;
import com.bi.website.mediaplay.format.MediaPlayInfohashJoinMR.MediaPlayInfoJoinMap;
import com.bi.website.mediaplay.format.MediaPlayInfohashJoinMR.MediaPlayInfoJoinReduce;
import com.bi.website.mediaplay.format.MediaPlayPartFormatMR;
import com.bi.website.videoplay.format.mr.VideoPlayPartFormatMR.VideoPlayFormatMap;
import com.bi.website.videoplay.format.mr.VideoPlayPartFormatMR.VideoPlayFormatReduce;

public class VideoPlayPartFormat extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = new Job(conf, "WatchVVPartFormat");
		job.setJarByClass(VideoPlayPartFormatMR.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(VideoPlayFormatMap.class);
		job.setReducerClass(VideoPlayFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(8);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		MediaPlayJoinArgs logArgs = new MediaPlayJoinArgs();
		int nRet = 0;
		try {
			logArgs.init("MediaPlayJoinFormat.jar");
			logArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		 }

		nRet = ToolRunner.run(new Configuration(), new VideoPlayPartFormat(),
				logArgs.getParamStrs());
		System.out.println(nRet);
	}

}
