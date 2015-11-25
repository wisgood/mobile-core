package com.bi.mobile.download.format.errordata;

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
import com.bi.mobile.download.format.dataenum.DownLoadEnum;

public class DownLoadFormatFilterByErrorMR {

	public static class DownLoadFormatFilterByErrorMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;
		private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;
		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;
		// private AbstractDMDAO<String, Map<ConstantEnum, String>>
		// dmInforHashRuleDAO = null;
		// private AbstractDMDAO<String, Map<ConstantEnum, String>>
		// dmServerInfoRuleDAO = null;
		private static Logger logger = Logger
				.getLogger(DownLoadFormatFilterByErrorMap.class.getName());

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			this.dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
			this.dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
			this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
			// this.dmInforHashRuleDAO = new DMInforHashRuleDAOImpl<String,
			// Map<ConstantEnum, String>>();
			// this.dmServerInfoRuleDAO = new DMServerInfoRuleDAOImpl<String,
			// Map<ConstantEnum, String>>();

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
				// String dmInforHashFilePath = context.getConfiguration().get(
				// ConstantEnum.DM_COMMON_INFOHASH_FILEPATH.name());
				// this.dmInforHashRuleDAO
				// .parseDMObj(new File(dmInforHashFilePath));
				// String dmServerFilePath = context.getConfiguration().get(
				// ConstantEnum.DM_MOBILE_SERVER_FILEPATH.name());
				// this.dmServerInfoRuleDAO.parseDMObj(new
				// File(dmServerFilePath));
			} else {
				System.out.println("DownLoadETLMap rummode is cluster!");
				this.dmPlatyRuleDAO.parseDMObj(new File(
						ConstantEnum.DM_MOBILE_PLATY.name().toLowerCase()));
				this.dmQuDaoRuleDAO.parseDMObj(new File(
						ConstantEnum.DM_MOBILE_QUDAO.name().toLowerCase()));
				this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE
						.name().toLowerCase()));
				// this.dmInforHashRuleDAO.parseDMObj(new File(
				// ConstantEnum.DM_COMMON_INFOHASH.name().toLowerCase()));
				// this.dmServerInfoRuleDAO.parseDMObj(new File(
				// ConstantEnum.DM_MOBILE_SERVER.name().toLowerCase()));
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

		public Map<String, String> getDownLoadFormatStr(String originalData) {
			// DownLoadETL downloadETL = null;
			Map<String, String> downloadETLMap = new HashMap<String, String>();
			String[] splitSts = originalData.split(",");
			if (splitSts.length <= DownLoadEnum.IH.ordinal()) {
				downloadETLMap
						.put("colcum lenght less "
								+ (DownLoadEnum.IH.ordinal() + 1), originalData);
			} else {
				try {
					// System.out.println(originalData);
//					String originalDataTranf = originalData.replaceAll(",",
//							"\t");
					// System.out.println(originalDataTranf);

					String tmpstampInfoStr = splitSts[DownLoadEnum.TIMESTAMP
							.ordinal()];
					java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
							.formatTimestamp(tmpstampInfoStr);
					// java.util.Map<ConstantEnum, String> formatTimesMap =
					// TimestampFormatMobileUtil.getFormatTimesMap(originalData,splitSts,
					// BootStrapEnum.class.getName());

					// dataId
					String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
					String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
					// if (dateId.equalsIgnoreCase("")) {
					// return null;
					// }
					// hourid
					int hourId = Integer.parseInt(hourIdStr);
					logger.info("dateId:" + dateId);
					logger.info("hourId:" + hourId);

					String platInfo = splitSts[DownLoadEnum.DEV.ordinal()];
					platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);
					// System.out.println(platInfo);

					// platid
					int platId = 0;
					platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
					logger.info("platId:" + platId);
					String versionInfo = splitSts[DownLoadEnum.VER.ordinal()];
					// System.out.println(versionInfo);
					// versionId
					long versionId = -0l;
					versionId = IPFormatUtil.ip2long(versionInfo);
					logger.info("versionId:" + versionId);
					// qudaoId
//					String qudaoInfo = splitSts[DownLoadEnum.SID.ordinal()];
					// int qudaoId = Integer.parseInt(qudaoInfo);
					// qudaoId = this.dmQuDaoRuleDAO.getDMOjb(qudaoId);
					// int qudaoId = SidFormatMobileUtil.getSid(qudaoInfo,
					// dmQuDaoRuleDAO);
					int qudaoId = SidFormatMobileUtil.getSidByEnum(splitSts,
							dmQuDaoRuleDAO, DownLoadEnum.class.getName());
					logger.info("qudaoInfo:" + qudaoId);
					// ipinfo
					String ipInfoStr = splitSts[DownLoadEnum.IP.ordinal()];
					long ipLong = 0;
					ipLong = IPFormatUtil.ip2long(ipInfoStr);

					java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
							.getDMOjb(ipLong);
					String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
					String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
					String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
					logger.info("ipinfo:" + ipLong + "::" + ipRuleMap);
					// mac地址
					String macInfoStr = splitSts[DownLoadEnum.MAC.ordinal()];
					MACFormatUtil.isCorrectMac(macInfoStr);
					String macCode = MACFormatUtil.macFormat(macInfoStr).get(
							ConstantEnum.MAC_LONG);
					logger.info("MacCode:" + macCode);

					// downLoadETLSB.append(dateId + "\t");
					// downLoadETLSB.append(hourId + "\t");
					// downLoadETLSB.append(platId + "\t");
					// downLoadETLSB.append(versionId + "\t");
					// downLoadETLSB.append(qudaoId + "\t");
					// downLoadETLSB.append(cityId + "\t");
					// downLoadETLSB.append(macCode + "\t");
					// downLoadETLSB.append(provinceId + "\t");
					// downLoadETLSB.append(ispId + "\t");
					// downLoadETLSB.append(originalDataTranf.trim());

				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error("error originalData:" + originalData);
					logger.error(e.getMessage(), e.getCause());
					downloadETLMap.put(e.getMessage(), originalData);
				}
			}
			return downloadETLMap;
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			try {
				String line = value.toString();
				Map<String, String> downloadETLMap = this
						.getDownLoadFormatStr(line);
				if (null != downloadETLMap && downloadETLMap.size() > 0) {
					Set<Map.Entry<String, String>> set = downloadETLMap
							.entrySet();
					for (Iterator<Map.Entry<String, String>> it = set
							.iterator(); it.hasNext();) {
						Map.Entry<String, String> entry = (Map.Entry<String, String>) it
								.next();
						context.write(new Text(entry.getKey()),
								new Text(entry.getValue()));
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
				context.write(new Text(e.getMessage()), value);
			}
		}
	}

	public static class DownLoadFormatFilterByErrorReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}

		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedExceptionDownLoadETLFilterByError
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(DownLoadFormatFilterByErrorMR.class);
		job.setJobName("DownLoadETLFilterByErrorMR");
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
		job.getConfiguration().set(
				ConstantEnum.DM_COMMON_INFOHASH_FILEPATH.name(),
				"conf/dm_common_infohash");
		job.getConfiguration().set(
				ConstantEnum.DM_MOBILE_SERVER_FILEPATH.name(),
				"conf/dm_mobile_server");
		FileInputFormat.addInputPath(job, new Path("input_download"));
		FileOutputFormat.setOutputPath(job, new Path(
				"output_validata_download"));
		job.setMapperClass(DownLoadFormatFilterByErrorMap.class);
		job.setReducerClass(DownLoadFormatFilterByErrorReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
