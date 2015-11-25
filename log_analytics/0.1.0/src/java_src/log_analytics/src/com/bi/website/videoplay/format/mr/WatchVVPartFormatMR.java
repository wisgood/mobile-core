package com.bi.website.videoplay.format.mr;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.website.videoplay.format.constant.WatchVVWebEnum;
import com.bi.website.videoplay.format.constant.WatchVVWebFormatEnum;

public class WatchVVPartFormatMR {
	public static class WatchVVPartFormatMap extends Mapper<LongWritable, Text, Text, Text> {

		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dimIpRuleDAO = null;
		private String flagId = "0";
		//private String ip_table_path = "";
		private static Logger logger = Logger.getLogger(WatchVVPartFormatMap.class.
				getName());

		protected void setup(Context context) throws IOException, InterruptedException {
	        // TODO Auto-generated method stub
			super.setup(context);
			this.dimIpRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();	
			this.dimIpRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name().toLowerCase()));
			this.flagId = context.getConfiguration().get("flags");
		}
	
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {						
			String originalData = value.toString();
			String[] rawsplitStr = originalData.split(",");			
			StringBuilder WatchVVETLStr = new StringBuilder();		
			if(rawsplitStr.length < WatchVVWebEnum.URL.ordinal()+1){
				StringBuilder rawData = new StringBuilder(originalData);
				for(int i = rawsplitStr.length; i < WatchVVWebEnum.URL.ordinal()+1; i++){				
					rawData.append("," + "0");	
					originalData = new String(rawData);
				}
			}
			String[] splitStr = originalData.split(",");
			String originalDataTrans = originalData.replaceAll(",", "\t");
			String dateId = "";
			String videoId = "";

			try {
				String tmpstampInfoStr = splitStr[WatchVVWebEnum.TIMESTAMP.ordinal()];
				java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
					.formatTimestamp(tmpstampInfoStr);
				
				dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
				String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
				if (dateId.equalsIgnoreCase("")) {
					dateId = "00000000";
				}
				int hourId = Integer.parseInt(hourIdStr);
				int platId = 0;
				if(Integer.parseInt(flagId) == 0){
					platId = 1;
				} else{
					platId = 2;
				}			
				String ipInfoStr = splitStr[WatchVVWebEnum.IP.ordinal()];
				long ipLong = 0;
				ipLong = IPFormatUtil.ip2long(ipInfoStr);

				java.util.Map<ConstantEnum, String> ipRuleMap = this.dimIpRuleDAO
						.getDMOjb(ipLong);
				String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
				String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
				String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
				WatchVVETLStr.append(dateId + "\t");
				WatchVVETLStr.append(provinceId + "\t");
				WatchVVETLStr.append(ispId + "\t");
				WatchVVETLStr.append(platId + "\t");
				WatchVVETLStr.append(cityId + "\t");
				WatchVVETLStr.append(flagId + "\t");
				videoId = splitStr[WatchVVWebEnum.MEDIAID.ordinal()];
				if(videoId == null){
					videoId = "0";
				}					
				WatchVVETLStr.append(videoId + "\t");
				WatchVVETLStr.append(originalDataTrans.trim());
			} catch (Exception e) {
				// TODO Auto-generated catch block
					//e.printStackTrace();
					logger.error("error originalData:" + originalData);
					logger.error(e.getMessage(), e.getCause());
				}
			String WatchVVStr = WatchVVETLStr.toString();

			if (null != WatchVVStr) {
				context.write(new Text(WatchVVStr.split("\t")[WatchVVWebFormatEnum.TIMESTAMP.ordinal()]), 
						new Text(WatchVVStr));
			}
		}

	}
		
	public static class WatchVVPartFormatReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, new Text());
			}
		}
   }
	
	public static void main(String[] args) throws Exception{	
		if(args.length != 2){
	   			System.err.println("Usage WatchVVPartFormat <input> <output>!");
				System.exit(1);
		}		
		Job job = new Job();
		job.setJarByClass(VideoPlayPartFormatMR.class);
		job.setJobName("WatchVVPartFormatMR");
		job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(), 
				"conf/ip_table");
		job.getConfiguration().set(WatchVVWebFormatEnum.FLAG_ID.name(),
				"1");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass( WatchVVPartFormatMap.class);
		job.setReducerClass( WatchVVPartFormatReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
