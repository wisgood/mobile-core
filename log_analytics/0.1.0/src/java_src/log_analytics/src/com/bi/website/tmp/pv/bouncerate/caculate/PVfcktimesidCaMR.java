/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PVfcktimesidCaMR.java 
 * @Package com.bi.website.tmp.pv.bouncerate.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-23 上午11:20:00 
 */
package com.bi.website.tmp.pv.bouncerate.caculate;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bi.comm.util.TimestampFormatUtil;

/**
 * @ClassName: PVfcktimesidCaMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-23 上午11:20:00
 */
public class PVfcktimesidCaMR {

	public static class PVfcktimesidCaMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String orginData = value.toString();
			String[] fields = orginData.split("\t");
			if (fields.length > PVNewEnum.SESSIONID.ordinal()) {
				String userFCKStr = fields[PVNewEnum.FCK.ordinal()].trim();
				String timestampStr = fields[PVNewEnum.TIMESTAMP.ordinal()]
						.trim();
				String sessionidStr = fields[PVNewEnum.SESSIONID.ordinal()]
						.trim();
				context.write(new Text(userFCKStr), new Text(timestampStr
						+ "\t" + sessionidStr));
			}
		}

	}

	public static class PVfcktimesidCaReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			TreeMap<Long, String> pvfcktimesTreeMap = new TreeMap<Long, String>();
			for (Text value : values) {
				String valueStr = value.toString();
				String[] fields = valueStr.split("\t");
				pvfcktimesTreeMap.put(new Long(fields[0]), fields[1]);
			}
			Iterator<Long> pvfcktimesIterator = pvfcktimesTreeMap.keySet()
					.iterator();
			while (pvfcktimesIterator.hasNext()) {
				// it.next()得到的是key，tm.get(key)得到obj
				Long timeStamp = pvfcktimesIterator.next();
				String sessionidStr = pvfcktimesTreeMap.get(timeStamp).trim();
				context.write(key, new Text(TimestampFormatUtil.formatTimeStamp(timeStamp+"") + "\t" + sessionidStr));
			}
		}

	}

	/**
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 * @Title: main
	 * @Description: 这里用一句话描述这个方法的作用
	 * @param @param args 参数说明
	 * @return void 返回类型说明
	 * @throws
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		String inputPaths = "input_new_pv";
		Job job = new Job();
		job.setJarByClass(PVfcktimesidCaMR.class);
		job.setJobName("PVfcktimesidCaMR");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		FileInputFormat.setInputPaths(job, inputPaths);
		FileOutputFormat.setOutputPath(job, new Path("output_new_pv"));
		job.setMapperClass(PVfcktimesidCaMapper.class);
		job.setReducerClass(PVfcktimesidCaReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
