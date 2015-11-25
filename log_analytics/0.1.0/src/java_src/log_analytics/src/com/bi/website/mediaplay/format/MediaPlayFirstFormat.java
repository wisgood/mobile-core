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
import org.apache.log4j.Logger;

import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.MACFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;



public class MediaPlayFirstFormat {

		public static class MediaPlayETLMap extends Mapper<LongWritable, Text, Text, Text> {
			
			private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

			private static Logger logger = Logger
					.getLogger(MediaPlayETLMap.class.getName());

			@Override
			protected void setup(Context context) throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				super.setup(context);

				this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
			

				if (this.isLocalRunMode(context)) {
					String dmMobilePlayFilePath = context.getConfiguration().get(
							ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name());
					
					String dmIpTableFilePath = context.getConfiguration().get(
							ConstantEnum.IPTABLE_FILEPATH.name());
					this.dmIPRuleDAO.parseDMObj(new File(dmIpTableFilePath));
	
				} else {
					System.out.println("FbufferETLMap rummode is cluster!");
					
					this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE
							.name().toLowerCase()));
					// this.dmInforHashRuleDAO.parseDMObj(new File(
					// ConstantEnum.DM_COMMON_INFOHASH.name().toLowerCase()));
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

			public String getMediaPlayETLStr(String originalData) {
				// FbufferETL fbufferETL = null;
				StringBuilder mediaplayETLStr = new StringBuilder();
				String[] splitSts = originalData.split(",");
				try {
					String originalDataTranf = originalData.replaceAll(",", "\t");
					// System.out.println(originalDataTranf);

					String tmpstampInfoStr = splitSts[MediaPlayEnum.TIMESTAMP
							.ordinal()];
					java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
							.formatTimestamp(tmpstampInfoStr);
					String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
					String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
					int hourId = Integer.parseInt(hourIdStr);
					logger.info("dateId:" + dateId);
					logger.info("hourId:" + hourId);
	              
					// platid
					int platId = 1;					
					
					String ipInfoStr = splitSts[MediaPlayEnum.IP.ordinal()];
					long ipLong = 0;
					ipLong = IPFormatUtil.ip2long(ipInfoStr);

					java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
							.getDMOjb(ipLong);
					String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
					String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
					String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
					logger.info("ipinfo:" + ipLong + "::" + ipRuleMap);
					String macInfoStr = splitSts[MediaPlayEnum.MAC.ordinal()];
					MACFormatUtil.isCorrectMac(macInfoStr);
					String macCode = MACFormatUtil.macFormat(macInfoStr).get(
							ConstantEnum.MAC_LONG);
					logger.info("MacCode:" + macCode);

					mediaplayETLStr.append(dateId + "\t");
					mediaplayETLStr.append(provinceId + "\t");
					mediaplayETLStr.append(ispId + "\t");
					mediaplayETLStr.append(platId + "\t");
					mediaplayETLStr.append(cityId + "\t");
					// fbufferETLStr.append(mediaId + "\t");
					// fbufferETLStr.append(serialId + "\t");
	
					mediaplayETLStr.append(originalDataTranf.trim());

				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error("error originalData:" + originalData);
					logger.error(e.getMessage(), e.getCause());
				}

				return mediaplayETLStr.toString();
			}

			public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
				String line = value.toString();
				String mediaplayStr = this.getMediaPlayETLStr(line);
				if (null != mediaplayStr && !("".equalsIgnoreCase(mediaplayStr))) {
					context.write(new Text(mediaplayStr.split("\t")[MediaPlayEtlEnum.TIMESTAMP
											.ordinal()]), new Text(mediaplayStr));
				}
			}
		}

		public static class MediaPlayEtlReduce extends
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
			job.setJarByClass(MediaPlayFirstFormat.class);
			job.setJobName("MediaPlayFirstFormat");

			job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(),
					"conf/ip_table");
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(MediaPlayETLMap.class);
			job.setReducerClass(MediaPlayEtlReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}


}
