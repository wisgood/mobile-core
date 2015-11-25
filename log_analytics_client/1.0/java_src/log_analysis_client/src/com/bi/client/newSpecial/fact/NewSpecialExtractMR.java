/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: NewSpecialChannelMR.java 
* @Package com.bi.client.newSpecial.fact 
* @Description: 从pgclick日志中抽取出属于newspecial的记录
* @author limm
* @date 2013-9-12 下午2:41:20 
* @input:/dw/logs/client/format/pgclick
* @output:/dw/logs/3_client/4_newSpecial/
* @executeCmd:hadoop jar ./jar/newSpecial.jar com.bi.client.newSpecial.fact.NewSpecialMR
* 															-D stat_date=日期
* 															-D input_dir=输入路径
* 															-D output_dir=输出路径
*/
package com.bi.client.newSpecial.fact;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.bi.client.pgclick.format.PgclickLogFormatEnum;

public class NewSpecialExtractMR extends Configured implements Tool{
	private static final String NEWSPECIAL_FLAG = "fs.funshion.com/newspecial";
	private static final String FOCUS_FLAG = "slider";
	private static final String LONGVIDEO_PLAY_FLAG = "fsp:";
	private static final String MICROVIDEO_PLAY_FLAG = "fs.funshion.com/video/play";
	private static final String LONGVIDEO = "1";
	private static final String MICROVIDEO = "2";
	
	public static class NewSpecialExtractMapper extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String ext = null;
			String mediaId = null;
			String provinceId = null;
			String hourId = null;
			String versionId = null;
			String videoType = null;
			String channelId = null;
			String mac= null;
			String newSpecial_play_flag = "1";
			String[] fields = value.toString().split("\t");
			String block = fields[PgclickLogFormatEnum.BLOCK.ordinal()];
			String url = fields[PgclickLogFormatEnum.URL.ordinal()];
			if(url.contains(NEWSPECIAL_FLAG) && !block.contains(FOCUS_FLAG)){   //url为新用户首页，且block不为焦点图
				ext = fields[PgclickLogFormatEnum.EXT.ordinal()];
				if(ext.contains(LONGVIDEO_PLAY_FLAG)){
					videoType = LONGVIDEO;
					String[] strs = ext.split("|");
					String[] mediaInfo = strs[5].split("=");
					mediaId = mediaInfo[1];
				}else if(ext.contains(MICROVIDEO_PLAY_FLAG)){
					videoType = MICROVIDEO;
					String[] strs = ext.split("/");
					mediaId = strs[5];
				}else{
					newSpecial_play_flag = "0";
				}
				
				if(newSpecial_play_flag.equals("1")){
					hourId = fields[PgclickLogFormatEnum.HOUR_ID.ordinal()];
					provinceId = fields[PgclickLogFormatEnum.PROVINCE_ID.ordinal()];
					versionId = fields[PgclickLogFormatEnum.VERSION_ID.ordinal()];
					mac = fields[PgclickLogFormatEnum.MAC.ordinal()];
					channelId = fields[PgclickLogFormatEnum.CHANNEL_ID.ordinal()];
					if(mediaId == null || !mediaId.matches("^\\d+$")){
						mediaId = "0";
					}
					newKey.set(mac);
					newValue.set(hourId + "," + provinceId + "," + versionId + "," +  channelId + "," + videoType + "," + mediaId);
					context.write(newKey, newValue); 
				}
			}
		}
	}
	
	public static class NewSpecialExtractReducer extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		private HashMap<String, Integer> dimMap = new HashMap<String, Integer>();
			
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			dimMap.clear();
			String dim = null;
			for(Text val : values){
				dim = val.toString();
				if(dimMap.containsKey(dim)){
					dimMap.put(dim, dimMap.get(dim)+1);
				}else{
					dimMap.put(dim, 1);
				}
			}
			StringBuilder dimSB = new StringBuilder();
			for(String mapKey : dimMap.keySet()){
				if(dimSB.length() > 0){
					dimSB.append("\t");
				}
				dimSB.append(mapKey).append(",").append(dimMap.get(mapKey));
			}
			newValue.set(dimSB.toString());
			context.write(key, newValue);
		}
	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();

		Job job = new Job(conf, "NewSpecialExtractMR");
		job.setJarByClass(NewSpecialExtractMR.class);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		String outputDir = conf.get("output_dir");
		Path tmpOutput = new Path(outputDir);
		FileOutputFormat.setOutputPath(job, tmpOutput);
		tmpOutput.getFileSystem(conf).delete(tmpOutput, true);

		job.setMapperClass(NewSpecialExtractMapper.class);
		job.setReducerClass(NewSpecialExtractReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(20);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
}
