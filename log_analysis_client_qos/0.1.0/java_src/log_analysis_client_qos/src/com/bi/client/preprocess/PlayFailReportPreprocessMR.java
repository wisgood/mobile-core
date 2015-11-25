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
import com.bi.client.datadefine.PlayFailReportEnum;
import com.bi.client.dimension.AbstractDimDAO;
import com.bi.client.dimension.DimIPTableDAO;
import com.bi.client.jargsparser.PreprocessMRArgsProcessor;
import com.bi.client.util.CleanField;
import com.bi.client.util.CleanField.FieldValueMessyCodeException;
import com.bi.client.util.DottedDecimalNotation;
import com.bi.client.util.DottedDecimalNotation.DottedDecimalNotationException;
import com.bi.client.util.PathProcessor;
import com.bi.client.util.TimeStamp;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class PlayFailReportPreprocessMR extends Configured implements Tool
{

	public static class PlayFailReportPreprocessMap extends Mapper<LongWritable, Text, Text, Text>
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
				//context.write(
				//		new Text(originRecord.split(",")[PlayFailReportEnum.O_TIMESTAMP.ordinal()]),
				//		new Text(originRecord));
			}
			else
			{
				context.write(
						new Text(formatRecord.split("\t")[PlayFailReportEnum.O_TIMESTAMP.ordinal()]),
						new Text(formatRecord));
			}
		}

		private String getFormatRecord(String originRecord)
		{
			String[] splitsOriginRecord = originRecord.trim().split(",");

			// 1 Filter & Clean each field
			String tmpStr = null;

			// 1.1 time_stamp
			long oTimeStamp = 0;
			try
			{
				tmpStr = splitsOriginRecord[PlayFailReportEnum.O_TIMESTAMP.ordinal()].trim();
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

			// 1.2 IP
			String oIP;
			try
			{
				tmpStr = splitsOriginRecord[PlayFailReportEnum.O_IP.ordinal()].trim();
				try
				{
					oIP = DottedDecimalNotation.format(tmpStr);
				}
				catch (DottedDecimalNotationException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oIP = "0.0.0.0";
			}

			// 1.3 MAC
			String oMac;
			try
			{
				tmpStr = splitsOriginRecord[PlayFailReportEnum.O_MAC.ordinal()].trim();
				try
				{
					oMac = CleanField.cleanMacFieldValue(tmpStr);
				}
				catch (FieldValueMessyCodeException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oMac = "UNDEFINED";
			}

			// 1.4 version
			String oVersion;
			try
			{
				tmpStr = splitsOriginRecord[PlayFailReportEnum.O_VERSION.ordinal()];
				tmpStr = tmpStr.replaceAll("[_a-zA-Z]*", "");
				try
				{
					oVersion = DottedDecimalNotation.format(tmpStr);
				}
				catch (DottedDecimalNotationException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oVersion = "0.0.0.0";
			}

			// 1.5 info_hash;
			String oInfoHash;
			try
			{
				tmpStr = splitsOriginRecord[PlayFailReportEnum.O_INFOHASH.ordinal()].trim();
				try
				{
					oInfoHash = CleanField.cleanHexFieldValue(tmpStr);
				}
				catch (FieldValueMessyCodeException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oInfoHash = "UNDEFINED";
			}

			// 1.6 fail_code
			int oFailCode;
			try
			{
				tmpStr = splitsOriginRecord[PlayFailReportEnum.O_FAILCODE.ordinal()].trim();
				if ("".equals(tmpStr))
				{
					oFailCode = -9;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oFailCode = tmpDouble.intValue();
					}
					catch (NumberFormatException e)
					{
						return CommonEnum.ERROR_RECORD.name();
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oFailCode = -9;
			}

			//2 transfrom from cleaned fields

			//2.1 DateID & HourID
			int tDateID = TimeStamp.getDateID(String.valueOf(oTimeStamp));
			int tHourID = TimeStamp.getHourID(String.valueOf(oTimeStamp));

			//2.2 tIP & tProvinceID & tCityID & tIspID
			long tIP;
			int tProvinceID;
			int tCityID;
			int tIspID;
			try
			{
				tIP = DottedDecimalNotation.dotDec2Dec(oIP);
			}
			catch (DottedDecimalNotationException e)
			{
				return CommonEnum.ERROR_RECORD.name();
			}
			try
			{
				Map<CommonEnum, String> transMap = dimIPTableDAO.getDimTransMap(tIP);
				tProvinceID = Integer.parseInt(transMap.get(CommonEnum.PROVINCE_ID));
				tCityID = Integer.parseInt(transMap.get(CommonEnum.CITY_ID));
				tIspID = Integer.parseInt(transMap.get(CommonEnum.ISP_ID));
			}
			catch (Exception e)
			{
				return CommonEnum.ERROR_RECORD.name();
			}

			// 2.3 tVersion
			long tVersion;
			try
			{
				tVersion = DottedDecimalNotation.dotDec2Dec(oVersion);
			}
			catch (DottedDecimalNotationException e)
			{
				return CommonEnum.ERROR_RECORD.name();
			}

			StringBuilder formatRecordBuffer = new StringBuilder();
			formatRecordBuffer.append(oTimeStamp + "\t");
			formatRecordBuffer.append(oIP + "\t");
			formatRecordBuffer.append(oMac + "\t");
			formatRecordBuffer.append(oVersion + "\t");
			formatRecordBuffer.append(oInfoHash + "\t");
			formatRecordBuffer.append(oFailCode + "\t");
			formatRecordBuffer.append(tDateID + "\t");
			formatRecordBuffer.append(tHourID + "\t");
			formatRecordBuffer.append(tIP + "\t");
			formatRecordBuffer.append(tProvinceID + "\t");
			formatRecordBuffer.append(tCityID + "\t");
			formatRecordBuffer.append(tIspID + "\t");
			formatRecordBuffer.append(tVersion);

			return formatRecordBuffer.toString();
		}
	}

	public static class PlayFailReportPreprocessReduce extends Reducer<Text, Text, Text, Text>
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

		Job job = new Job(conf, "PlayFailReportPreprocessMR");
		job.setJarByClass(PlayFailReportPreprocessMR.class);
		job.setInputFormatClass(LzoTextInputFormat.class);

		FileInputFormat.setInputPaths(job, PathProcessor.listHourPaths(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.setMapperClass(PlayFailReportPreprocessMap.class);
		job.setReducerClass(PlayFailReportPreprocessReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(7);

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
		nRet = ToolRunner.run(new Configuration(), new PlayFailReportPreprocessMR(),
				argsProcessor.getOptionValueArray());
		System.out.println(nRet);
	}

}
