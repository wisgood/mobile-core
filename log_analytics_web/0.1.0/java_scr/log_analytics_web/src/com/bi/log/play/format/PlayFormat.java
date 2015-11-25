package com.bi.log.play.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.log.play.format.PlayFormatMR.PlayFormatMap;
import com.bi.log.play.format.PlayFormatMR.PlayFormatReduce;
import com.bi.log.format.param.LogArgs;


public class PlayFormat extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "PlayFormatMR");
		job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
		job.setJarByClass(PlayFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//FileOutputFormat.setCompressOutput(job, true); // 设置输出结果采用压缩
		//FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class); // 设置输出结果采用lzo压缩
		job.setMapperClass(PlayFormatMap.class);
		job.setReducerClass(PlayFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(18);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LogArgs logArgs = new LogArgs();
		int nRet = 0;
		try {
			logArgs.init("PlayFormat.jar");
			logArgs.parse(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			logArgs.getParser().printUsage();
			System.exit(1);
		}

		nRet = ToolRunner.run(new Configuration(), new PlayFormat(),
				logArgs.getParamStrs());
		System.out.println(nRet);
	}

}
