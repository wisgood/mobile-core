package com.bi.client.preprocess;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.client.datadefine.CommonEnum;
import com.bi.client.datadefine.LoginEnum;
import com.bi.client.dimension.AbstractDimDAO;
import com.bi.client.dimension.DimIPTableDAO;
import com.bi.client.jargsparser.PreprocessMRArgsProcessor;
import com.bi.client.util.CleanField;
import com.bi.client.util.CleanField.FieldValueMessyCodeException;
import com.bi.client.util.DottedDecimalNotation;
import com.bi.client.util.DottedDecimalNotation.DottedDecimalNotationException;
import com.bi.client.util.PathProcessor;
import com.bi.client.util.TimeStamp;

public class LoginPreprocessMR extends Configured implements Tool
{

	public static class PreprocessMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		private AbstractDimDAO dimIPTableDAO = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			this.dimIPTableDAO = new DimIPTableDAO();
			File dimIPFile = new File(CommonEnum.IP_TABLE.name().toLowerCase());
			this.dimIPTableDAO.parseDimFile(dimIPFile);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException
		{
			String originRecord = value.toString().trim();
			String formatRecord = this.getFormatRecord(originRecord);

			if (CommonEnum.ERROR_RECORD.name().equals(formatRecord))
			{
				//context.write(new Text(originRecord.split("\t")[LoginEnum.O_TIMESTAMP.ordinal()]),
				//		new Text(originRecord));
			}
			else
			{
				context.write(new Text(formatRecord.split("\t")[LoginEnum.O_TIMESTAMP.ordinal()]),
						new Text(formatRecord));
			}
		}

		private String getFormatRecord(String originRecord)
		{
			String[] splitsOriginRecord = originRecord.trim().split("\t");

			// 1 Filter & Clean each field
			String tmpStr = null;

			// 1.1 session_id
			long oSessionID;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_SESSIONID.ordinal()].trim();
				try
				{
					oSessionID = Long.parseLong(tmpStr);
				}
				catch (NumberFormatException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				return CommonEnum.ERROR_RECORD.name();
			}

			// 1.2 client_ip
			String oClientIP;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_CLIENTIP.ordinal()].trim();
				try
				{
					long dec = Long.parseLong(tmpStr);
					try
					{
						oClientIP = DottedDecimalNotation.dec2DotDec(dec);
					}
					catch (DottedDecimalNotationException e)
					{
						return CommonEnum.ERROR_RECORD.name();
					}
				}
				catch (NumberFormatException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				return CommonEnum.ERROR_RECORD.name();
			}

			// 1.3 time_stamp
			long oTimeStamp;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_TIMESTAMP.ordinal()].trim();
				try
				{
					oTimeStamp = TimeStamp.getSecTimeStamp(tmpStr);
				}
				catch (NumberFormatException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				return CommonEnum.ERROR_RECORD.name();
			}

			// 1.4 package_id
			long oPackageID;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_PACKAGEID.ordinal()].trim();
				try
				{
					oPackageID = Long.parseLong(tmpStr);
				}
				catch (NumberFormatException e)
				{
					oPackageID = -9;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oPackageID = -9;
			}

			// 1.5 pvs
			long oPVS;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_PVS.ordinal()].trim();
				try
				{
					oPVS = Long.parseLong(tmpStr);
				}
				catch (NumberFormatException e)
				{
					oPVS = -9;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oPVS = -9;
			}

			// 1.6 login_mode
			long oLoginMode;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_LOGINMODE.ordinal()].trim();
				try
				{
					oLoginMode = Long.parseLong(tmpStr);
				}
				catch (NumberFormatException e)
				{
					oLoginMode = -9;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oLoginMode = -9;
			}

			// 1.7 login_num
			long oLoginNum;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_LOGINNUM.ordinal()].trim();
				try
				{
					oLoginNum = Long.parseLong(tmpStr);
				}
				catch (NumberFormatException e)
				{
					oLoginNum = -9;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oLoginNum = -9;
			}

			// 1.8 mac
			String oMac;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_MAC.ordinal()].trim();
				try
				{
					oMac = CleanField.cleanMacFieldValue(tmpStr);
				}
				catch (FieldValueMessyCodeException e)
				{
					oMac = "UNDEFINED";
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oMac = "UNDEFINED";
			}

			// 1.9 reason
			long oReason;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_REASON.ordinal()].trim();
				try
				{
					oReason = Long.parseLong(tmpStr);
				}
				catch (NumberFormatException e)
				{
					oReason = -9;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oReason = -9;
			}

			// 1.10 server_ip
			String oServerIP;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_SERVERIP.ordinal()].trim();
				try
				{
					long dec = Long.parseLong(tmpStr);
					try
					{
						oServerIP = DottedDecimalNotation.dec2DotDec(dec);
					}
					catch (DottedDecimalNotationException e)
					{
						oServerIP = "0.0.0.0";
					}
				}
				catch (NumberFormatException e)
				{
					oServerIP = "0.0.0.0";
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oServerIP = "0.0.0.0";
			}

			// 1.11 time_used
			long oTimeUsed;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_TIMEUSED.ordinal()].trim();
				try
				{
					oTimeUsed = Long.parseLong(tmpStr);
				}
				catch (NumberFormatException e)
				{
					oTimeUsed = -9;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oTimeUsed = -9;
			}

			// 1.12 version
			String oVersion;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_VERSION.ordinal()];
				//tmpStr = tmpStr.replaceAll("[_a-zA-Z]*", "");
				try
				{
					oVersion = DottedDecimalNotation.format(tmpStr);
				}
				catch (DottedDecimalNotationException e)
				{
					oVersion = "0.0.0.0";
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oVersion = "0.0.0.0";
			}

			// 1.13 province_id
			long oProvinceID;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_PROVINCEID.ordinal()].trim();
				try
				{
					oProvinceID = Long.parseLong(tmpStr);
				}
				catch (NumberFormatException e)
				{
					oProvinceID = -9;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oProvinceID = -9;
			}

			// 1.14 area_id
			long oAreaID;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_AREAID.ordinal()].trim();
				try
				{
					oAreaID = Long.parseLong(tmpStr);
				}
				catch (NumberFormatException e)
				{
					oAreaID = -9;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oAreaID = -9;
			}

			// 1.15 isp_id
			long oIspID;
			try
			{
				tmpStr = splitsOriginRecord[LoginEnum.O_ISPID.ordinal()].trim();
				try
				{
					oIspID = Long.parseLong(tmpStr);
				}
				catch (NumberFormatException e)
				{
					oIspID = -9;
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oIspID = -9;
			}

			//2 transfrom from cleaned fields

			//2.1 DateID & HourID
			int tDateID = TimeStamp.getDateID(String.valueOf(oTimeStamp));
			int tHourID = TimeStamp.getHourID(String.valueOf(oTimeStamp));

			//2.2 tIP & tProvinceID & tCityID & tIspID
			long tClientIP;
			int tProvinceID;
			int tCityID;
			int tIspID;
			try
			{
				tClientIP = DottedDecimalNotation.dotDec2Dec(oClientIP);
			}
			catch (DottedDecimalNotationException e)
			{
				return CommonEnum.ERROR_RECORD.name();
			}
			try
			{
				Map<CommonEnum, String> transMap = dimIPTableDAO.getDimTransMap(tClientIP);
				tProvinceID = Integer.parseInt(transMap.get(CommonEnum.PROVINCE_ID));
				tCityID = Integer.parseInt(transMap.get(CommonEnum.CITY_ID));
				tIspID = Integer.parseInt(transMap.get(CommonEnum.ISP_ID));
			}
			catch (Exception e)
			{
				return CommonEnum.ERROR_RECORD.name();
			}

			StringBuilder formatRecordBuffer = new StringBuilder();
			formatRecordBuffer.append(oSessionID + "\t");
			formatRecordBuffer.append(oClientIP + "\t");
			formatRecordBuffer.append(oTimeStamp + "\t");
			formatRecordBuffer.append(oPackageID + "\t");
			formatRecordBuffer.append(oPVS + "\t");
			formatRecordBuffer.append(oLoginMode + "\t");
			formatRecordBuffer.append(oLoginNum + "\t");
			formatRecordBuffer.append(oMac + "\t");
			formatRecordBuffer.append(oReason + "\t");
			formatRecordBuffer.append(oServerIP + "\t");
			formatRecordBuffer.append(oTimeUsed + "\t");
			formatRecordBuffer.append(oVersion + "\t");
			formatRecordBuffer.append(oProvinceID + "\t");
			formatRecordBuffer.append(oAreaID + "\t");
			formatRecordBuffer.append(oIspID + "\t");
			formatRecordBuffer.append(tDateID + "\t");
			formatRecordBuffer.append(tHourID + "\t");
			formatRecordBuffer.append(tClientIP + "\t");
			formatRecordBuffer.append(tProvinceID + "\t");
			formatRecordBuffer.append(tCityID + "\t");
			formatRecordBuffer.append(tIspID);

			return formatRecordBuffer.toString();
		}
	}

	public static class PreprocessReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException
		{
			for (Text value : values)
			{
				context.write(value, new Text());
			}
		}
	}

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();

		String mrPath = LoginPreprocessMR.class.getName();
		int lastDotPos = mrPath.lastIndexOf(".") + 1;
		String mrName = mrPath.substring(lastDotPos);

		Job job = new Job(conf, mrName);
		job.setJarByClass(LoginPreprocessMR.class);
		//job.setInputFormatClass(LzoTextInputFormat.class);

		FileInputFormat.setInputPaths(job, PathProcessor.listHourPaths(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.setMapperClass(PreprocessMapper.class);
		job.setReducerClass(PreprocessReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(30);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] argv) throws Exception
	{
		PreprocessMRArgsProcessor argsProcessor = new PreprocessMRArgsProcessor();
		try
		{
			argsProcessor.initDefaultOptions("log_analytics_client_qos.jar");
			argsProcessor.parseAndCheckArgs(argv);
		}
		catch (Exception e)
		{
			System.out.println(e.toString());
			argsProcessor.getAutoHelpParser().printMRJarUsage();
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new LoginPreprocessMR(),
				argsProcessor.getOptionValueArray());
		System.out.println(nRet);
	}
}
