package com.bi.client.fsplayafter.format.correctdata;

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
import com.bi.comm.util.MACFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;


import com.bi.client.fsplayafter.format.dataenum.FsplayAfterETLEnum;
import com.bi.client.fsplayafter.format.dataenum.FsplayAfterEnum;

public class FsplayAfterOutIHFormatMR {
	
	public static class FsplayAfterOutIHFormatMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;
		//private AbstractDMDAO<String, Map<ConstantEnum, String>> dmInforHashRuleDAO = null;
		private static Logger logger = Logger.getLogger(FsplayAfterOutIHFormatMap.class
				.getName());

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
			//this.dmInforHashRuleDAO = new DMInforHashRuleDAOImpl<String, Map<ConstantEnum, String>>();
			if (this.isLocalRunMode(context)) {
				String dmIpTableFilePath = context.getConfiguration().get(
						ConstantEnum.IPTABLE_FILEPATH.name());
				this.dmIPRuleDAO.parseDMObj(new File(dmIpTableFilePath));
//				String dmInforHashFilePath = context.getConfiguration().get(
//						ConstantEnum.DM_COMMON_INFOHASH_FILEPATH.name());
//				this.dmInforHashRuleDAO
//						.parseDMObj(new File(dmInforHashFilePath));
			} else {
				System.out.println("FsplayAfterETLMap rummode is cluster!");
				this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE
						.name().toLowerCase()));
//				this.dmInforHashRuleDAO.parseDMObj(new File(
//						ConstantEnum.DM_COMMON_INFOHASH.name().toLowerCase()));
			}
		}

		private boolean isLocalRunMode(Context context) {

			String mapredJobTrackerMode = context.getConfiguration().get(
					"mapred.job.tracker");
			if (null != mapredJobTrackerMode
					&& ConstantEnum.LOCAL.name().equalsIgnoreCase(
							mapredJobTrackerMode)) {
				return true;
			}
			return false;
		}

// process once
		public String getFsplayAfterETLStr(String originalData) {
			// FbufferETL fbufferETL = null;
			StringBuilder fsplayAfterETLStr = new StringBuilder();
			String[] splitSts = originalData.split(",");
			if (splitSts.length <= FsplayAfterEnum.IH.ordinal()) {
				return null;
			}
			try {
				// System.out.println(originalData);
				String originalDataTranf = originalData.replaceAll(",", "\t");
				// System.out.println(originalDataTranf);

				String tmpstampInfoStr = splitSts[FsplayAfterEnum.TIMESTAMP
						.ordinal()];
				java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
						.formatTimestamp(tmpstampInfoStr);
				// java.util.Map<ConstantEnum, String> formatTimesMap =
				// TimestampFormatMobileUtil.getFormatTimesMap(originalData,splitSts,
				// BootStrapEnum.class.getName());
				// dataId
				String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
				String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
				if (dateId.equalsIgnoreCase("")) {
					return null;
				}
				// hourid
				int hourId = Integer.parseInt(hourIdStr);
				logger.info("dateId:" + dateId);
				logger.info("hourId:" + hourId);

				// ipinfo
				String ipInfoStr = splitSts[FsplayAfterEnum.IP.ordinal()];
				long ipLong = 0;
				ipLong = IPFormatUtil.ip2long(ipInfoStr);

				java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
						.getDMOjb(ipLong);
				String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
				String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
				String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
				logger.info("ipinfo:" + ipLong + "::" + ipRuleMap);
				// mac地址
				String macInfoStr = splitSts[FsplayAfterEnum.MAC.ordinal()];
				String macCode = MACFormatUtil.macFormat(macInfoStr).get(
						ConstantEnum.MAC_LONG);
				logger.info("MacCode:" + macCode);

//				// inforhash
//				String inforHashStr = splitSts[FbufferEnum.IH.ordinal()];
//				java.util.Map<ConstantEnum, String> inforHashMap = this.dmInforHashRuleDAO
//						.getDMOjb(inforHashStr);
//				logger.info("inforhash:" + inforHashMap);
//				// channelId
//				int channelId = Integer.parseInt(inforHashMap
//						.get(ConstantEnum.CHANNEL_ID));
//				logger.info("channelId:" + channelId);
//				// serialID
//				String serialId = inforHashMap.get(ConstantEnum.SERIAL_ID);
//				logger.info("serialId:" + serialId);
//				// meidaID
//				String mediaId = inforHashMap.get(ConstantEnum.MEIDA_ID);
//				logger.info("meidaId:" + mediaId);
				int platId = 2;
				fsplayAfterETLStr.append(dateId + "\t");
				fsplayAfterETLStr.append(hourId + "\t");
				fsplayAfterETLStr.append(platId + "\t");
//				fsplayAfterETLStr.append(channelId + "\t");
				fsplayAfterETLStr.append(cityId + "\t");
				fsplayAfterETLStr.append(macCode + "\t");
//				fsplayAfterETLStr.append(mediaId + "\t");
//				fsplayAfterETLStr.append(serialId + "\t");
				fsplayAfterETLStr.append(provinceId + "\t");
				fsplayAfterETLStr.append(ispId + "\t");
				fsplayAfterETLStr.append(originalDataTranf.trim());

			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("error originalData:" + originalData);
				logger.error(e.getMessage(), e.getCause());
			}

			return fsplayAfterETLStr.toString();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String fsplayAfterETLStr = this.getFsplayAfterETLStr(line);
			if (null != fsplayAfterETLStr && !("".equalsIgnoreCase(fsplayAfterETLStr))) {
				context.write(
						new Text(
								fsplayAfterETLStr.split("\t")[FsplayAfterETLEnum.TIMESTAMP
										.ordinal()]), new Text(fsplayAfterETLStr));
			}
		}
	}

	public static class FsplayAfterOutIHFormatReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, new Text());
			}

		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(FsplayAfterOutIHFormatMR.class);
		job.setJobName("FsplayAfterETL");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		// 设置配置文件默认路径
		job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(),
				"conf/ip_table");
		job.getConfiguration().set(
				ConstantEnum.DM_COMMON_INFOHASH_FILEPATH.name(),
				"conf/dm_common_infohash");
		FileInputFormat.addInputPath(job, new Path("input_fsplayafter"));
		FileOutputFormat.setOutputPath(job, new Path("output_fsplayafter"));
		job.setMapperClass(FsplayAfterOutIHFormatMap.class);
		job.setReducerClass(FsplayAfterOutIHFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
