package com.bi.mobile.exit.format.errordata;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
import com.bi.mobile.exit.format.dataenum.ExitEnum;

public class ExitFormatFilterByErrorMR {

	public static class ExitFormatFilterByErrorMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;
		private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;
		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;
		private static Logger logger = Logger
				.getLogger(ExitFormatFilterByErrorMap.class.getName());

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

		public Map<String, String> getExitFormatFilterByErrorStr(
				String originalData) {
			// Text Text = null;
			Map<String, String> exitETLFilterMap = new HashMap<String, String>();
			String[] splitSts = originalData.split(",");
			if (splitSts.length <= ExitEnum.TN.ordinal()) {
				exitETLFilterMap.put("colcum lenght less "+(ExitEnum.TN.ordinal()+1), originalData);
			} else {

				try {
					// System.out.println(originalDataTranf);
					String timstampInfoStr = splitSts[ExitEnum.TIMESTAMP
							.ordinal()];
					java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
							.formatTimestamp(timstampInfoStr);
					// java.util.Map<ConstantEnum, String> formatTimesMap =
					// TimestampFormatMobileUtil.getFormatTimesMap(originalData,splitSts,
					// BootStrapEnum.class.getName());
					// dataId
					String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
					String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
//					if (dateId.equalsIgnoreCase("")) {
//						return null;
//					}
					// hourid
					int hourId = Integer.parseInt(hourIdStr);
					logger.info("dateId:" + dateId);
					logger.info("hourId:" + hourId);

					// 获取设备类型
					String platInfo = splitSts[ExitEnum.DEV.ordinal()];
					platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);
					// System.out.println(platInfo);
					// platid
					int platId = 0;
					platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
					logger.info("platId:" + platId);
					String versionInfo = splitSts[ExitEnum.VER.ordinal()];
					// System.out.println(versionInfo);
					// versionId
					long versionId = -0l;
					versionId = IPFormatUtil.ip2long(versionInfo);
					logger.info("versionId:" + versionId);
					// qudaoId
//					String qudaoInfo = splitSts[ExitEnum.SID.ordinal()];
//					int qudaoId = Integer.parseInt(qudaoInfo);
//					qudaoId = this.dmQuDaoRuleDAO.getDMOjb(qudaoId);
//					int qudaoId = SidFormatMobileUtil.getSid(qudaoInfo, dmQuDaoRuleDAO);
					int qudaoId =SidFormatMobileUtil.getSidByEnum(splitSts, dmQuDaoRuleDAO, ExitEnum.class.getName());
					logger.info("qudaoInfo:" + qudaoId);
					// ipinfo
					String ipInfoStr = splitSts[ExitEnum.IP.ordinal()];
					long ipLong = 0;
					ipLong = IPFormatUtil.ip2long(ipInfoStr);
					java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
							.getDMOjb(ipLong);
					logger.info("ipinfo:" + ipRuleMap);
					// mac地址
					String macInfoStr = splitSts[ExitEnum.MAC.ordinal()];
					MACFormatUtil.isCorrectMac(macInfoStr);
//					String macCode = MACFormatUtil.macFormat(macInfoStr).get(
//							ConstantEnum.MAC_LONG);
//					logger.info("MacCode:" + macCode);
					String macInfor = MACFormatUtil.macFormatToCorrectStr(macInfoStr);

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					exitETLFilterMap.put(e.getMessage(), originalData);
				}
			}

			return exitETLFilterMap;
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			Map<String, String> exitETLMap = this
					.getExitFormatFilterByErrorStr(line);
			if (null != exitETLMap && exitETLMap.size() > 0) {
				Set<Map.Entry<String, String>> set = exitETLMap.entrySet();
				for (Iterator<Map.Entry<String, String>> it = set.iterator(); it
						.hasNext();) {
					Map.Entry<String, String> entry = (Map.Entry<String, String>) it
							.next();
					context.write(new Text(entry.getKey()),
							new Text(entry.getValue()));
				}
			}
		}
	}

	public static class ExitFormatFilterByErrorReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(ExitFormatFilterByErrorMR.class);
		job.setJobName("ExitETLFilterByErrorMR");
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
		FileInputFormat.addInputPath(job, new Path("input_exit"));
		FileOutputFormat.setOutputPath(job, new Path("output_validate_exit"));
		job.setMapperClass(ExitFormatFilterByErrorMap.class);
		job.setReducerClass(ExitFormatFilterByErrorReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
