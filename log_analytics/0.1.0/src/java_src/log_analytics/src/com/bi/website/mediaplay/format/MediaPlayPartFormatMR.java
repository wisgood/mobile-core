package com.bi.website.mediaplay.format;

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

import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;

public class MediaPlayPartFormatMR {
	
	public static class MediaPlayEtlMap extends Mapper<LongWritable, Text, Text, Text> {

		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dimIpRuleDAO = null;

		protected void setup(Context context) throws IOException, InterruptedException {
	        // TODO Auto-generated method stub
			super.setup(context);
			this.dimIpRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();		
			this.dimIpRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name().toLowerCase()));
		}
				
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String originalData = value.toString();
			String originalDataTrans = originalData.replaceAll(",", "\t");
			StringBuilder MediaPlayETLStr = new StringBuilder();
			String[] splitSts = originalData.split(",");
			String dateId = "";

			try {
				String tmpstampInfoStr = splitSts[MediaPlayEnum.TIMESTAMP.ordinal()];
				java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
					.formatTimestamp(tmpstampInfoStr);
				
				dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
				String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
				if (dateId.equalsIgnoreCase("")) {
					dateId = "0000000";
				}
				int hourId = Integer.parseInt(hourIdStr);
				
				int platId = 1;
				String ipInfoStr = splitSts[MediaPlayEnum.IP.ordinal()];
				long ipLong = 0;
				ipLong = IPFormatUtil.ip2long(ipInfoStr);

				java.util.Map<ConstantEnum, String> ipRuleMap = this.dimIpRuleDAO
						.getDMOjb(ipLong);
				String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
				String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
				String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
			
				MediaPlayETLStr.append(dateId + "\t");
				MediaPlayETLStr.append(provinceId + "\t");
				MediaPlayETLStr.append(ispId + "\t");
				MediaPlayETLStr.append(platId + "\t");
				MediaPlayETLStr.append(cityId + "\t");
				MediaPlayETLStr.append(originalDataTrans.trim());
			} catch (Exception e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}
			String MediaPlayStr = MediaPlayETLStr.toString();

			if (null != MediaPlayStr) {
				context.write(new Text(MediaPlayStr.split("\t")[MediaPlayEtlEnum.TIMESTAMP.ordinal()]), new Text(MediaPlayStr));
			}
		}

	}
		
	public static class MediaPlayEtlReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, new Text());
			}
		}
   }
	
	public static void main(String[] args) throws Exception{	
		if(args.length != 2){
	   			System.err.println("Usage MediaPlayPartEtl Test <input> <output>!");
				System.exit(1);
		}		
		Job job = new Job();
		job.setJarByClass(MediaPlayPartFormatMR.class);
		job.setJobName("MediaPlayPartFormatMR");
		job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(), "conf/ip_table");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MediaPlayEtlMap.class);
		job.setReducerClass(MediaPlayEtlReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
