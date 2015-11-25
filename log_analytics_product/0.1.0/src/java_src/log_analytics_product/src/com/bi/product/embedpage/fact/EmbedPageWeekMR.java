/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: CooperatePageMR.java 
* @Package com.bi.client.cooperatePage.fact.week 
* @Description: 计算合作页面的播放量、媒体下载量、客户端下载量
* @author limm
* @date 2013-10-17 下午5:10:11 
* @input:pgclick日志：/dw/logs/client/format/pgclick/2013/10/10
* @output:/dw/logs/3_client/5_cooperatePage/
*/
package com.bi.product.embedpage.fact;

import java.io.IOException;

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
import org.apache.hadoop.util.ToolRunner;

public class EmbedPageWeekMR extends Configured implements Tool{
	private static final String MEDIA_PLAY_EXT_91 = "turnurl=http://www.funshion.com/app/91mobile/play.html?mid=";
	private static final String MEDIA_PLAY_EXT_360 = "turnurl=http://app.funshion.com/app/aphone/play.html?mid=";
	private static final String MEDIA_PLAY_EXT_WDJ = "turnurl=http://app.funshion.com/app/wdjaphone/play.html?mid=";
	private static final String MEDIA_PAGE_URL_91 = "www.funshion.com/app/91mobile/media.html?mid=";
	private static final String MEDIA_PLAY_URL_91 = "www.funshion.com/app/91mobile/play.html?mid=";
	private static final String FIRST_PAGE_URL_91 = "www.funshion.com/app/91mobile/index.html";	
	private static final String MOVIE_PAGE_URL_91 = "www.funshion.com/app/91mobile/list.html?type=movie";
	private static final String ENTERTAINMENT_PAGE_URL_91 = "www.funshion.com/app/91mobile/list.html?type=variety";
	private static final String MEDIA_PAGE_URL_360 = "app.funshion.com/app/aphone/media.html?mid=";
	private static final String MEDIA_PLAY_URL_360 = "app.funshion.com/app/aphone/play.html?mid=";
	private static final String FIRST_PAGE_URL_360 = "app.funshion.com/app/aphone/index.html";	
	private static final String MOVIE_PAGE_URL_360 = "app.funshion.com/app/aphone/list.html?type=movie";
	private static final String ENTERTAINMENT_PAGE_URL_360 = "app.funshion.com/app/aphone/list.html?type=variety";
	private static final String MEDIA_PAGE_URL_WDJ = "app.funshion.com/app/wdjaphone/media.html?mid=";
	private static final String MEDIA_PLAY_URL_WDJ = "app.funshion.com/app/wdjaphone/play.html?mid=";
	private static final String FIRST_PAGE_URL_WDJ = "app.funshion.com/app/wdjaphone/index.html";	
	private static final String MOVIE_PAGE_URL_WDJ = "app.funshion.com/app/wdjaphone/list.html?type=movie";
	private static final String ENTERTAINMENT_PAGE_URL_WDJ = "app.funshion.com/app/wdjaphone/list.html?type=variety";
	private static final String MEDIA_DOWNLOAD_BLOCK_REX_FIRST = ".*t_v_list.*~3.*~A.*";
	private static final String MEDIA_DOWNLOAD_BLOCK_SECOND = "dbtn_big";
	private static final String MEDIA_DOWNLOAD_EXT_FLAG = "turnurl=";
	private static final String CLIENT_DOWNLOAD_BLOCK_FIRST = "js-slider~A";
	private static final String CLIENT_DOWNLOAD_BLOCK_SECOND = "5!2~A";
	private static final String CLIENT_DOWNLOAD_BLOCK_THIRD = "t_v_banner~A";
	private static final String CLIENT_DOWNLOAD_BLOCK_FORTH = "t_v_src~A!2";
	private static final String CLIENT_DOWNLOAD_BLOCK_FIFTH = "5!3~A";
	private static final String FLAG_91 = "www.funshion.com/app/91mobile";
	private static final String FLAG_360 = "app.funshion.com/app/aphone";
	private static final String FLAG_WDJ = "app.funshion.com/app/wdjaphone";
	private static final String INDEX_91 = "1";
	private static final String INDEX_360 = "2";
	private static final String INDEX_WDJ = "3";
	
	public static class EmbedPageWeekMap extends
			Mapper<LongWritable, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();
		private String date = null;
		
		@Override
		public void setup(Context context){
			date = context.getConfiguration().get("stat_date");
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String playFlag = "0";
			String mediaDownloadFlag = "0";
			String clientDownloadFlag = "0";
			String cooperateChannelId = null;
			String ext = null;
			String url = null;
			String block = null;
			String[] fields = value.toString().split("\t");
			block = fields[PgclickLogFormatEnum.BLOCK.ordinal()];
			url = fields[PgclickLogFormatEnum.URL.ordinal()];
			ext = fields[PgclickLogFormatEnum.EXT.ordinal()];
			if(url.contains(FLAG_91)){
				cooperateChannelId = INDEX_91;
				clientDownloadFlag = isClientDownloadBlock(block);
				if(ext.contains(MEDIA_PLAY_EXT_91)){
					playFlag = "1";
				}
				if(url.contains(MEDIA_PAGE_URL_91) || url.contains(FIRST_PAGE_URL_91)){
					mediaDownloadFlag = isMediaDownloadBlockMediaPage(block, ext);
				}else if(url.contains(MEDIA_PLAY_URL_91) || url.contains(MOVIE_PAGE_URL_91)){
					mediaDownloadFlag = isMediaDownloadBlockPlayPage(block, ext);
				}else if(url.contains(ENTERTAINMENT_PAGE_URL_91)){
					mediaDownloadFlag = isMediaDownloadBlockEntertainmentPage(block, ext);
				}
			}else if(url.contains(FLAG_360)){
				cooperateChannelId = INDEX_360;
				clientDownloadFlag = isClientDownloadBlock360(block);
				if(ext.contains(MEDIA_PLAY_EXT_360)){
					playFlag = "1";
				}
				if(url.contains(MEDIA_PAGE_URL_360) || url.contains(FIRST_PAGE_URL_360)){
					mediaDownloadFlag = isMediaDownloadBlockMediaPage(block, ext);
				}else if(url.contains(MEDIA_PLAY_URL_360) || url.contains(MOVIE_PAGE_URL_360)){
					mediaDownloadFlag = isMediaDownloadBlockPlayPage(block, ext);
				}else if(url.contains(ENTERTAINMENT_PAGE_URL_360)){
					mediaDownloadFlag = isMediaDownloadBlockEntertainmentPage(block, ext);
				}
			}else if(url.contains(FLAG_WDJ)){
				cooperateChannelId = INDEX_WDJ;
				clientDownloadFlag = isClientDownloadBlock(block);
				if(ext.contains(MEDIA_PLAY_EXT_WDJ)){
					playFlag = "1";
				}
				if(url.contains(MEDIA_PAGE_URL_WDJ) || url.contains(FIRST_PAGE_URL_WDJ)){
					mediaDownloadFlag = isMediaDownloadBlockMediaPage(block, ext);
				}else if(url.contains(MEDIA_PLAY_URL_WDJ) || url.contains(MOVIE_PAGE_URL_WDJ)){
					mediaDownloadFlag = isMediaDownloadBlockPlayPage(block, ext);
				}else if(url.contains(ENTERTAINMENT_PAGE_URL_WDJ)){
					mediaDownloadFlag = isMediaDownloadBlockEntertainmentPage(block, ext);
				}
			}
			if(playFlag.equals("1") || mediaDownloadFlag.equals("1") || clientDownloadFlag.equals("1")){
				newKey.set(date + "\t" + cooperateChannelId);
				newValue.set(playFlag + "\t" + mediaDownloadFlag + "\t" + clientDownloadFlag);
				context.write(newKey, newValue);				
			}
		}
		
		public String isClientDownloadBlock360(String block){
			String ret = "0";
			if(block.contains(CLIENT_DOWNLOAD_BLOCK_FIRST) || block.contains(CLIENT_DOWNLOAD_BLOCK_SECOND) 
					|| block.contains(CLIENT_DOWNLOAD_BLOCK_THIRD) || block.contains(CLIENT_DOWNLOAD_BLOCK_FORTH)
					|| block.contains(CLIENT_DOWNLOAD_BLOCK_FIFTH)){
				ret = "1";
			}
			return ret;
		}
		
		public String isClientDownloadBlock(String block){
			String ret = "0";
			if(block.contains(CLIENT_DOWNLOAD_BLOCK_FIRST) || block.contains(CLIENT_DOWNLOAD_BLOCK_SECOND) 
					|| block.contains(CLIENT_DOWNLOAD_BLOCK_THIRD) || block.contains(CLIENT_DOWNLOAD_BLOCK_FORTH)){
				ret = "1";
			}
			return ret;
		}
		
		public String isMediaDownloadBlockEntertainmentPage(String block, String ext){
			String ret = "0";
			if((block.matches(MEDIA_DOWNLOAD_BLOCK_REX_FIRST)) && ext.equals(MEDIA_DOWNLOAD_EXT_FLAG)){
				ret = "1";
			}
			return ret;
		}
		
		public String isMediaDownloadBlockMediaPage(String block, String ext){
			String ret = "0";
			if((block.matches(MEDIA_DOWNLOAD_BLOCK_REX_FIRST) || block.contains(MEDIA_DOWNLOAD_BLOCK_SECOND)) && ext.equals(MEDIA_DOWNLOAD_EXT_FLAG)){
				ret = "1";
			}
			return ret;
		}
		
		public String isMediaDownloadBlockPlayPage(String block, String ext){
			String ret = "0";
			if(block.contains(MEDIA_DOWNLOAD_BLOCK_SECOND) && ext.equals(MEDIA_DOWNLOAD_EXT_FLAG)){
				ret =  "1";
			}
			return ret;
		}
	}
	
	public static class EmbedPageWeekReduce extends
			Reducer<Text, Text, Text, Text>{
		private Text newValue = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			long playNum = 0;
			long clientDownloadNum = 0;
			long mediaDownloadNum = 0;
			for(Text val : values){
				String[] fields = val.toString().split("\t");
				playNum += Long.parseLong(fields[0]);
				mediaDownloadNum += Long.parseLong(fields[1]);
				clientDownloadNum += Long.parseLong(fields[2]);
			}
			newValue.set(playNum + "\t" + mediaDownloadNum + "\t" + clientDownloadNum);
			context.write(key, newValue);
		}
	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();

		Job job = new Job(conf, "EmbedPageWeekMR");
		job.setJarByClass(EmbedPageWeekMR.class);
		conf.set("stat_date", conf.get("stat_date"));
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		String outputDir = conf.get("output_dir");
		Path output = new Path(outputDir);
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);

		job.setMapperClass(EmbedPageWeekMap.class);
		job.setCombinerClass(EmbedPageWeekReduce.class);
		job.setReducerClass(EmbedPageWeekReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new EmbedPageWeekMR(), args);
		System.out.println(res);
	}
}
