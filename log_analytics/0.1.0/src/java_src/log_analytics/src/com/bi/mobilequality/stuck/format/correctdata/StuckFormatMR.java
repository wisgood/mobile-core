package com.bi.mobilequality.stuck.format.correctdata;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import com.bi.mobile.comm.dm.pojo.dao.DMServerInfoRuleDAOImpl;
import com.bi.mobilequality.stuck.format.dataenum.StuckFormatEnum;
import com.bi.mobilequality.stuck.format.dataenum.StuckOriginEnum;

/**
 * @ClassName: StuckFormatMR
 * @Description: Clean and transform stuck raw log data to generate basic data
 * @author wangzg
 * @date 2013-5-15
 */
public class StuckFormatMR
{
	public static final String FLASH_RECORD = "flash_record";
	public static final String ERROR_RECORD = "error_record";

	/**
	 * @ClassName: StuckFormatMap
	 * @Description: Static inner classes
	 */
	public static class StuckFormatMap extends Mapper<LongWritable, Text, Text, Text>
	{
		// Data Structure of storing dimension files data
		private AbstractDMDAO<Long, Map<ConstantEnum, String>> dimIPRuleDAO = null;
		private AbstractDMDAO<String, Integer> dimPlatRuleDAO = null;
		// private AbstractDMDAO<String, Map<ConstantEnum, String>> dimInfoHashRuleDAO = null;
		private AbstractDMDAO<String, Map<ConstantEnum, String>> dimServerRuleDAO = null;

		// Logging information
		private static Logger logger = Logger.getLogger(StuckFormatMap.class.getName());

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			this.dimIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
			this.dimPlatRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
			// this.dimInfoHashRuleDAO = new DMInforHashRuleDAOImpl<String, Map<ConstantEnum, String>>();
			this.dimServerRuleDAO = new DMServerInfoRuleDAOImpl<String, Map<ConstantEnum, String>>();

			// StuckFormatMap run mode is cluster!
			logger.info("StuckFormatMap run mode is cluster!");
			File dimIPFile = new File(ConstantEnum.IP_TABLE.name().toLowerCase());
			File dimPlatFile = new File(ConstantEnum.DM_MOBILE_PLATY.name().toLowerCase());
			// File dimInfoHashFile = new File(ConstantEnum.DM_COMMON_INFOHASH.name().toLowerCase());
			File dimServerFile = new File(ConstantEnum.DM_MOBILE_SERVER.name().toLowerCase());

			this.dimIPRuleDAO.parseDMObj(dimIPFile);
			this.dimPlatRuleDAO.parseDMObj(dimPlatFile);
			// this.dimInfoHashRuleDAO.parseDMObj(dimInfoHashFile);
			this.dimServerRuleDAO.parseDMObj(dimServerFile);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String originRecord = value.toString().trim();
			String formatRecord = this.getFormatRecord(originRecord);

			if (StuckFormatMR.ERROR_RECORD.equals(formatRecord))
			{
				//context.write(new Text(originRecord.split(",")[StuckOriginEnum.O00_TimeStamp.ordinal()]), new Text(originRecord));
			}
			else if (StuckFormatMR.FLASH_RECORD.equals(formatRecord))
			{
				
			}
			else
			{
				context.write(new Text(formatRecord.split("\t")[StuckFormatEnum.TIMESTAMP.ordinal()]), new Text(formatRecord));
			}
		}

		/**
		 * @param originRecord
		 * @return formatRecord
		 */
		private String getFormatRecord(String originRecord)
		{
			String[] splitsOriginRecord = originRecord.split(",");

			// 1. Filter out records
			// Special processing on records that 'device' field with 'flash' value
			// 'device' field format is: <device type>_<device os>_<device model>
			String tmpDevice;
			try
			{
				tmpDevice = splitsOriginRecord[StuckOriginEnum.DEVICE.ordinal()] + "_";
			}
			catch (IndexOutOfBoundsException e)
			{
				return StuckFormatMR.ERROR_RECORD;
				// return originRecord;
			}
			int tmpFirstUnderline = tmpDevice.indexOf("_");
			String tmpDeviceType;
			try
			{
				tmpDeviceType = tmpDevice.substring(0, tmpFirstUnderline).toLowerCase();
			}
			catch (IndexOutOfBoundsException e)
			{
				return StuckFormatMR.ERROR_RECORD;
				// return originRecord;
			}
			if (-1 != tmpDeviceType.indexOf("flash"))
			{
				return StuckFormatMR.FLASH_RECORD;
			}

			// 2. Adjust the order of fields
			// Special processing with 'iphone' 'ipod' 'ipad' version '1.2.0.1' & '1.2.0.2'
			if (-1 != tmpDeviceType.indexOf("ip"))
			{
				if (13 == splitsOriginRecord.length)
				{
					String version = splitsOriginRecord[splitsOriginRecord.length - 2];
					if ("1.2.0.1".equals(version) || "1.2.0.2".equals(version))
					{
						for (int i = splitsOriginRecord.length - 2; i >= 5; i--)
						{
							splitsOriginRecord[i] = splitsOriginRecord[i - 1];
						}
						splitsOriginRecord[4] = version;
						// StringBuilder tmpBuf = new StringBuilder();
						// for (String field : splitsOriginRecord)
						// {
						// tmpBuf.append(field + ",");
						// }
						// return tmpBuf.toString();
					}
				}
			}

			// Special processing on 15 fields condition without field 'stuck_reason' & 'player_type'
			String[] splitsOriginRecordAdjust;
			if (15 == splitsOriginRecord.length)
			{
				splitsOriginRecordAdjust = new String[15 + 2];
				String tmpUserIP = splitsOriginRecord[14];
				Pattern pattern = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
				Matcher matcher = pattern.matcher(tmpUserIP);
				if (matcher.matches())
				{
					for (int i = 0; i <= 12; i++)
					{
						splitsOriginRecordAdjust[i] = splitsOriginRecord[i];
					}
					for (int i = 13; i <= 14; i++)
					{
						splitsOriginRecordAdjust[i] = "";
					}
					for (int i = 15; i <= 16; i++)
					{
						splitsOriginRecordAdjust[i] = splitsOriginRecord[i - 2];
					}
					splitsOriginRecord = splitsOriginRecordAdjust;
				}
			}

			// 3. Clean and transform each field
			String tmpStr = null;

			// 3.00-02 Transform from field 'timestamp'
			long oTimeStamp;
			long fDateID;
			long fHourID;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.TIMESTAMP.ordinal()];
				Map<ConstantEnum, String> timeStampTransMap = TimestampFormatUtil.formatTimestamp(tmpStr);
				oTimeStamp = Long.parseLong(timeStampTransMap.get(ConstantEnum.TIMESTAMP));
				fDateID = Long.parseLong(timeStampTransMap.get(ConstantEnum.DATE_ID));
				fHourID = Long.parseLong(timeStampTransMap.get(ConstantEnum.HOUR_ID));
			}
			catch (IndexOutOfBoundsException e)
			{
				return StuckFormatMR.ERROR_RECORD;
			}

			// 3.03-07 Transform from field 'ip'
			String oIP;
			long fIP;
			long fProvinceID;
			long fCityID;
			long fIspID;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.IP.ordinal()];
				oIP = IPFormatUtil.ipFormat(tmpStr);
				fIP = IPFormatUtil.ip2long(oIP);
				try
				{
					Map<ConstantEnum, String> ipTransMap = this.dimIPRuleDAO.getDMOjb(fIP);
					fProvinceID = Long.parseLong(ipTransMap.get(ConstantEnum.PROVINCE_ID));
					fCityID = Long.parseLong(ipTransMap.get(ConstantEnum.CITY_ID));
					fIspID = Long.parseLong(ipTransMap.get(ConstantEnum.ISP_ID));
				}
				catch (Exception e)
				{
					return StuckFormatMR.ERROR_RECORD;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oIP = "0.0.0.0";
				fIP = 0;
				fProvinceID = -1;
				fCityID = -1;
				fIspID = -1;
			}

			// 3.08-09 Transform from field 'device'
			String oDevice = tmpDeviceType;
			long fDeviceID;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.DEVICE.ordinal()];
				tmpStr = PlatTypeFormatUtil.getFormatPlatType(tmpStr);
				try
				{
					fDeviceID = this.dimPlatRuleDAO.getDMOjb(tmpStr);
				}
				catch (Exception e)
				{
					return StuckFormatMR.ERROR_RECORD;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				fDeviceID = 7;
			}

			// 3.10 Format fields
			String oMAC;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.MAC.ordinal()];
				// if field value is null, then set default value
				// if field missing, then set default value
				try
				{
					if ("".equals(tmpStr) || null == tmpStr)
					{
						oMAC = "000000000000";
					}
					else
					{
						MACFormatUtil.isCorrectMac(tmpStr);
						oMAC = MACFormatUtil.macFormatToCorrectStr(tmpStr);
					}
				}
				catch (Exception e)
				{
					return StuckFormatMR.ERROR_RECORD;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oMAC = "000000000000";
			}

			// 3.11-12
			String oVersion;
			long fVersion;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.VERSION.ordinal()];
				oVersion = IPFormatUtil.ipFormat(tmpStr);
				fVersion = IPFormatUtil.ip2long(oVersion);
			}
			catch (IndexOutOfBoundsException e)
			{
				oVersion = "0.0.0.0";
				fVersion = 0;
			}

			// 3.13
			long oNetworkType;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.NETWORKTYPE.ordinal()];
				// if field value is null, then set default value
				// if field value is not a number, then filter out this record
				// if field missing, then set default value
				if ("".equals(tmpStr) || null == tmpStr)
				{
					oNetworkType = -1;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oNetworkType = tmpDouble.longValue();
						// oNetworkType = Long.parseLong(tmpStr);
					}
					catch (NumberFormatException e)
					{
						return StuckFormatMR.ERROR_RECORD;
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oNetworkType = -1;
			}

			// 3.14-17 transform from field 'infohash'
			String oInfoHash;
			long fSerialID = -1;
			long fMediaID = -1;
			long fChannelID = -1;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.INFOHASH.ordinal()];
				// if field value is null, then set default value
				// if field missing, then set default value
				if ("".equals(tmpStr) || null == tmpStr)
				{
					oInfoHash = "UNDEFINED";
				}
				else
				{
					oInfoHash = tmpStr.toUpperCase();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oInfoHash = "UNDEFINED";
				fSerialID = -1;
				fMediaID = -1;
				fChannelID = -1;
			}

			// 3.18-20
			String oServerIP;
			long fServerIP;
			long fRoomID;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.SERVERIP.ordinal()];
				oServerIP = IPFormatUtil.ipFormat(tmpStr);
				fServerIP = IPFormatUtil.ip2long(oServerIP);
				try
				{
					Map<ConstantEnum, String> serverTransMap = this.dimServerRuleDAO.getDMOjb(fServerIP + "");
					fRoomID = Long.parseLong(serverTransMap.get(ConstantEnum.SERVERROOM_ID));
				}
				catch (Exception e)
				{
					return StuckFormatMR.ERROR_RECORD;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oServerIP = "0.0.0.0";
				fServerIP = 0;
				fRoomID = -1;
			}

			// 3.21
			long oBufferOK;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.BUFFEROK.ordinal()];
				// if field value is null, then set default value
				// if field value is not a number, then filter out this record
				// if field missing, then set default value
				if ("".equals(tmpStr) || null == tmpStr)
				{
					oBufferOK = -2;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oBufferOK = tmpDouble.longValue();
						// oBufferOK = Long.parseLong(tmpStr);
					}
					catch (NumberFormatException e)
					{
						return StuckFormatMR.ERROR_RECORD;
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oBufferOK = -2;
			}

			// 3.22
			long oStuckPosition;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.STUCKPOSITION.ordinal()];
				if ("".equals(tmpStr) || null == tmpStr)
				{
					oStuckPosition = 0;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oStuckPosition = tmpDouble.longValue();
						// oStuckPosition = Long.parseLong(tmpStr);
					}
					catch (NumberFormatException e)
					{
						// oStuckPosition = -99999;
						return StuckFormatMR.ERROR_RECORD;
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oStuckPosition = 0;
			}

			// 3.23
			long oStuckTime;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.STUCKTIME.ordinal()];
				if ("".equals(tmpStr) || null == tmpStr)
				{
					oStuckTime = 0;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oStuckTime = tmpDouble.longValue();
					}
					catch (NumberFormatException e)
					{
						return StuckFormatMR.ERROR_RECORD;
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oStuckTime = 0;
			}

			// 3.24
			long oDownloadRate;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.DOWNLOADRATE.ordinal()];
				if ("".equals(tmpStr) || null == tmpStr)
				{
					oDownloadRate = -1;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oDownloadRate = tmpDouble.longValue();
					}
					catch (NumberFormatException e)
					{
						return StuckFormatMR.ERROR_RECORD;
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oDownloadRate = -1;
			}

			// 3.25
			long oStreamID;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.STREAMID.ordinal()];
				if ("".equals(tmpStr) || null == tmpStr)
				{
					oStreamID = 1;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oStreamID = tmpDouble.longValue();
					}
					catch (NumberFormatException e)
					{
						oStreamID = 1;
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oStreamID = 1;
			}

			// 3.26
			long oStuckReason;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.STUCKREASION.ordinal()];
				if ("".equals(tmpStr) || null == tmpStr)
				{
					oStuckReason = 0;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oStuckReason = tmpDouble.longValue();
					}
					catch (NumberFormatException e)
					{
						return StuckFormatMR.ERROR_RECORD;
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oStuckReason = 0;
			}

			// 3.27
			long oPlayerType;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.PLAYERTYPE.ordinal()];
				if ("".equals(tmpStr) || null == tmpStr)
				{
					oPlayerType = -1;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oPlayerType = tmpDouble.longValue();
					}
					catch (NumberFormatException e)
					{
						//oPlayerType = -99999;
						return StuckFormatMR.ERROR_RECORD;
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oPlayerType = -1;
			}

			// 3.28
			long oReportTime;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.REPORTTIME.ordinal()];
				try
				{
					Double tmpDouble = new Double(tmpStr);
					oReportTime = tmpDouble.longValue();
				}
				catch (NumberFormatException e)
				{
					return StuckFormatMR.ERROR_RECORD;
				}
				Map<ConstantEnum, String> reportTimeTransMap = TimestampFormatUtil.formatRTTimeInfo(tmpStr);
				oReportTime = Long.parseLong(reportTimeTransMap.get(ConstantEnum.TIMESTAMP));
			}
			catch (IndexOutOfBoundsException e)
			{
				oReportTime = -1;
			}

			// 3.29-30
			String oUserIP;
			long fUserIP;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.USERIP.ordinal()];
				oUserIP = IPFormatUtil.ipFormat(tmpStr);
				fUserIP = IPFormatUtil.ip2long(oVersion);
			}
			catch (IndexOutOfBoundsException e)
			{
				oUserIP = "0.0.0.0";
				fUserIP = 0;
			}

			// 3.31
			long oClarity;
			try
			{
				tmpStr = splitsOriginRecord[StuckOriginEnum.CLARITY.ordinal()];
				if ("".equals(tmpStr) || null == tmpStr)
				{
					oClarity = -1;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oClarity = tmpDouble.longValue();
					}
					catch (NumberFormatException e)
					{
						return StuckFormatMR.ERROR_RECORD;
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oClarity = -1;
			}

			StringBuilder formatRecordBuffer = new StringBuilder();
			formatRecordBuffer.append(oTimeStamp + "\t");
			formatRecordBuffer.append(fDateID + "\t");
			formatRecordBuffer.append(fHourID + "\t");
			formatRecordBuffer.append(oIP + "\t");
			formatRecordBuffer.append(fIP + "\t");
			formatRecordBuffer.append(fProvinceID + "\t");
			formatRecordBuffer.append(fCityID + "\t");
			formatRecordBuffer.append(fIspID + "\t");
			formatRecordBuffer.append(oDevice + "\t");
			formatRecordBuffer.append(fDeviceID + "\t");
			formatRecordBuffer.append(oMAC + "\t");
			formatRecordBuffer.append(oVersion + "\t");
			formatRecordBuffer.append(fVersion + "\t");
			formatRecordBuffer.append(oNetworkType + "\t");
			formatRecordBuffer.append(oInfoHash + "\t");
			formatRecordBuffer.append(fSerialID + "\t");
			formatRecordBuffer.append(fMediaID + "\t");
			formatRecordBuffer.append(fChannelID + "\t");
			formatRecordBuffer.append(oServerIP + "\t");
			formatRecordBuffer.append(fServerIP + "\t");
			formatRecordBuffer.append(fRoomID + "\t");
			formatRecordBuffer.append(oBufferOK + "\t");
			formatRecordBuffer.append(oStuckPosition + "\t");
			formatRecordBuffer.append(oStuckTime + "\t");
			formatRecordBuffer.append(oDownloadRate + "\t");
			formatRecordBuffer.append(oStreamID + "\t");
			formatRecordBuffer.append(oStuckReason + "\t");
			formatRecordBuffer.append(oPlayerType + "\t");
			formatRecordBuffer.append(oReportTime + "\t");
			formatRecordBuffer.append(oUserIP + "\t");
			formatRecordBuffer.append(fUserIP + "\t");
			formatRecordBuffer.append(oClarity);

			return formatRecordBuffer.toString();
		}
	}

	/**
	 * @ClassName: StuckFormatReduce
	 * @Description: Static inner classes
	 */
	public static class StuckFormatReduce extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for (Text value : values)
			{
				context.write(value, new Text());
			}
		}
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception
	{
		Job job = new Job();
		job.setJarByClass(StuckFormatMR.class);
		job.setJobName("StuckFormatMR");
		job.getConfiguration().set(ConstantEnum.IP_TABLE.name(), args[2]);
		job.getConfiguration().set(ConstantEnum.DM_MOBILE_PLATY.name(), args[3]);
		// job.getConfiguration().set(ConstantEnum.DM_COMMON_INFOHASH_FILEPATH.name(), "dimensions/dm_common_infohash");
		job.getConfiguration().set(ConstantEnum.DM_MOBILE_SERVER.name(), args[4]);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(StuckFormatMap.class);
		job.setReducerClass(StuckFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(7);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
