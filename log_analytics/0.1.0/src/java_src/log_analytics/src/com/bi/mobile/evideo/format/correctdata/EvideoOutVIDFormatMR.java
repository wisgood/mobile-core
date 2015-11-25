package com.bi.mobile.evideo.format.correctdata;

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
import com.bi.comm.util.PlatTypeFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMQuDaoRuleDAOImpl;
import com.bi.mobile.comm.util.SidFormatMobileUtil;
import com.bi.mobile.comm.util.SpecialVersionRecomposeFormatMobileUtil;
import com.bi.mobile.evideo.format.dataenum.EvideoOutVIDFormatEnum;
import com.bi.mobile.evideo.format.dataenum.EvideoEnum;


public class EvideoOutVIDFormatMR {

	public static class EvideoFormatMap extends Mapper<LongWritable, Text, Text, Text> {

		private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;
		private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;
		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;
		private static Logger logger = Logger.getLogger(EvideoFormatMap.class
				.getName());
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			this.dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
			this.dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
			this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
			if (this.isLocalRunMode(context)) {
				String dmMobilePlayFilePath = context.getConfiguration().get(
						ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name());
				this.dmPlatyRuleDAO.parseDMObj(new File(dmMobilePlayFilePath));
				String dmQuodaoFilePath = context.getConfiguration().get(
						ConstantEnum.DM_MOBILE_QUDAO_FILEPATH.name());
				this.dmQuDaoRuleDAO.parseDMObj(new File(dmQuodaoFilePath));
				String dmIpTableFilePath = context.getConfiguration().get(
						ConstantEnum.IPTABLE_FILEPATH.name());
				this.dmIPRuleDAO.parseDMObj(new File(dmIpTableFilePath));
			} else {
				logger.info("TextMap rummode is cluster!");
				File dmMobilePlayFile = new File(ConstantEnum.DM_MOBILE_PLATY
						.name().toLowerCase());
				this.dmPlatyRuleDAO.parseDMObj(dmMobilePlayFile);
				this.dmQuDaoRuleDAO.parseDMObj(new File(
						ConstantEnum.DM_MOBILE_QUDAO.name().toLowerCase()));
				this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE
						.name().toLowerCase()));
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

		public String getEvideoFormatStr(String originalData) {
			// Text Text = null;
			StringBuilder exitETLSB = new StringBuilder();
			String[] splitSts = originalData.split(",");
			/*
			if (splitSts.length <= EvideoEnum.TYPE.ordinal()) {
				return null;

			}
			*/
			try {
				String originalDataTranf = originalData.replaceAll(",", "\t");
				if(splitSts.length >= EvideoEnum.SID.ordinal()){
					splitSts = SpecialVersionRecomposeFormatMobileUtil.recomposeBySpecialVersion(splitSts, EvideoEnum.class.getName(),"SID");
				}
				//System.out.println(originalDataTranf);
				String timstampInfoStr = splitSts[EvideoEnum.TIMESTAMP.ordinal()];
				java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
						.formatTimestamp(timstampInfoStr);
//				java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatMobileUtil.getFormatTimesMap(originalData,splitSts, BootStrapEnum.class.getName());
				// dataId
				String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
				String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
//				if (dateId.equalsIgnoreCase("")) {
//					return null;
//				}
				// hourid
				int hourId = Integer.parseInt(hourIdStr);
				logger.info("dateId:" + dateId);
				logger.info("hourId:" + hourId);

				// 获取设备类型
				String platInfo = splitSts[EvideoEnum.DEV.ordinal()];
				platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);
				//System.out.println(platInfo);
				// platid
				int platId = 0;
				platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
				logger.info("platId:" + platId);
				
	
				String versionInfo = splitSts[EvideoEnum.VER.ordinal()];
				//System.out.println(versionInfo);
				// versionId
				long versionId = -0l;
				versionId = IPFormatUtil.ip2long(versionInfo);
				logger.info("versionId:" + versionId);
				// qudaoId
//				String qudaoInfo = splitSts[ExitEnum.SID.ordinal()];
//				int qudaoId = Integer.parseInt(qudaoInfo);
//				qudaoId = this.dmQuDaoRuleDAO.getDMOjb(qudaoId);
//				int qudaoId = SidFormatMobileUtil.getSid(qudaoInfo, dmQuDaoRuleDAO);
				int qudaoId =SidFormatMobileUtil.getSidByEnum(splitSts, dmQuDaoRuleDAO, EvideoEnum.class.getName());
				logger.info("qudaoInfo:" + qudaoId);
				// ipinfo
				String ipInfoStr = splitSts[EvideoEnum.IP.ordinal()];
				long ipLong = 0;
				ipLong = IPFormatUtil.ip2long(ipInfoStr);
				java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
						.getDMOjb(ipLong);
				String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
				String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
				String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
				logger.info("ipinfo:" + ipRuleMap);
				// mac地址
				String macInfoStr = splitSts[EvideoEnum.MAC.ordinal()];
				MACFormatUtil.isCorrectMac(macInfoStr);
//				String macCode = MACFormatUtil.macFormat(macInfoStr).get(
//						ConstantEnum.MAC_LONG);
//				logger.info("MacCode:" + macCode);
				String macInfor = MACFormatUtil.macFormatToCorrectStr(macInfoStr);
				int flagId = 1;
				exitETLSB.append(dateId + "\t");
				exitETLSB.append(hourId + "\t");
				exitETLSB.append(flagId + "\t");
				exitETLSB.append(platId + "\t");
				exitETLSB.append(versionId + "\t");
				exitETLSB.append(qudaoId + "\t");
				exitETLSB.append(cityId + "\t");
				exitETLSB.append(macInfor + "\t");
				exitETLSB.append(provinceId + "\t");
				exitETLSB.append(ispId+ "\t");
				exitETLSB.append(originalDataTranf.trim() );

			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("error originalData:"+originalData);
				logger.error(e.getMessage(), e.getCause());
			}

			return exitETLSB.toString();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String evideoETLStr = this.getEvideoFormatStr(line);
			if (null != evideoETLStr && !("".equalsIgnoreCase(evideoETLStr))) {
				context.write(new Text(
						evideoETLStr.split("\t")[EvideoOutVIDFormatEnum.TIMESTAMP.ordinal()]),
						new Text(evideoETLStr));
			}
		}
	}

	public static class EvideoFormatReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, new Text());
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(EvideoOutVIDFormatMR.class);
		job.setJobName("EvideoFormatMR");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		// 设置配置文件默认路径
		job.getConfiguration().set(
				ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name(),
				"conf/dm_mobile_platy");
		job.getConfiguration().set(
				ConstantEnum.DM_MOBILE_QUDAO_FILEPATH.name(),
				"conf/dm_mobile_qudao");
		job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(),
				"conf/ip_table");
		FileInputFormat.addInputPath(job, new Path("input_evideo"));
		FileOutputFormat.setOutputPath(job, new Path("output_evideo"));
		job.setMapperClass(EvideoFormatMap.class);
		job.setReducerClass(EvideoFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
