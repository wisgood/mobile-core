package com.bi.ios.exchange.preprocess;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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

import com.bi.ios.exchange.datadefine.CommonEnum;
import com.bi.ios.exchange.datadefine.SpreadEnum;
import com.bi.ios.exchange.dimension.AbstractDimDAO;
import com.bi.ios.exchange.dimension.DimIPTableDAO;
import com.bi.ios.exchange.jargsparser.PreprocessMRArgsProcessor;
import com.bi.ios.exchange.util.DecodeStringField;
import com.bi.ios.exchange.util.DottedDecimalNotation;
import com.bi.ios.exchange.util.DottedDecimalNotation.DottedDecimalNotationException;
import com.bi.ios.exchange.util.PathProcessor;
import com.bi.ios.exchange.util.TimeStamp;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class SpreadPreprocessMR extends Configured implements Tool
{

	public static class SpreadPreprocessMap extends Mapper<LongWritable, Text, Text, Text>
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
				//context.write(new Text(originRecord.split(",")[SpreadEnum.O_TIMESTAMP.ordinal()]),
				//		new Text(originRecord));
			}
			else
			{
				context.write(new Text(formatRecord.split("\t")[SpreadEnum.O_TIMESTAMP.ordinal()]),
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
				tmpStr = splitsOriginRecord[SpreadEnum.O_TIMESTAMP.ordinal()].trim();
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
				tmpStr = splitsOriginRecord[SpreadEnum.O_IP.ordinal()].trim();
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

			// 1.3 device
			String oDevice;
			try
			{
				tmpStr = splitsOriginRecord[SpreadEnum.O_DEVICE.ordinal()].trim();
				try
				{
					oDevice = DecodeStringField.decode(tmpStr, "UTF-8");
					oDevice = oDevice.replaceAll("\\s{1,}", "-");
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oDevice = "UNDEFINED";
			}

			// 1.4 STREAMID
			long oStreamID;
			try
			{
				tmpStr = splitsOriginRecord[SpreadEnum.O_STREAMID.ordinal()].trim();
				tmpStr = tmpStr.replaceAll("[^0-9]", "");
				if ("".equals(tmpStr))
				{
					oStreamID = 0;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oStreamID = tmpDouble.intValue();
					}
					catch (NumberFormatException e)
					{
						return CommonEnum.ERROR_RECORD.name();
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oStreamID = 0;
			}

			// 1.5 ENVIP;
			String oEnvIP;
			try
			{
				tmpStr = splitsOriginRecord[SpreadEnum.O_ENVIP.ordinal()].trim();
				try
				{
					oEnvIP = DottedDecimalNotation.format(tmpStr);
				}
				catch (DottedDecimalNotationException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oEnvIP = "0.0.0.0";
			}

			// 1.6 APPURL1
			String oAppURL1;
			try
			{
				tmpStr = splitsOriginRecord[SpreadEnum.O_APPURL1.ordinal()].trim();
				try
				{
					oAppURL1 = DecodeStringField.decode(tmpStr, "UTF-8");
					oAppURL1 = oAppURL1.replaceAll("\\s{1,}", "");
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oAppURL1 = "UNDEFINED";
			}

			// 1.7 APPURL2
			String oAppURL2;
			try
			{
				tmpStr = splitsOriginRecord[SpreadEnum.O_APPURL2.ordinal()].trim();
				try
				{
					oAppURL2 = DecodeStringField.decode(tmpStr, "UTF-8");
					oAppURL2 = oAppURL2.replaceAll("\\s{1,}", "");
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oAppURL2 = "UNDEFINED";
			}

			// 1.8 APPURL3
			String oAppURL3;
			try
			{
				tmpStr = splitsOriginRecord[SpreadEnum.O_APPURL3.ordinal()].trim();
				try
				{
					oAppURL3 = DecodeStringField.decode(tmpStr, "UTF-8");
					oAppURL3 = oAppURL3.replaceAll("\\s{1,}", "-");
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oAppURL3 = "UNDEFINED";
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

			StringBuilder formatRecordBuffer = new StringBuilder();
			formatRecordBuffer.append(oTimeStamp + "\t");
			formatRecordBuffer.append(oIP + "\t");
			formatRecordBuffer.append(oDevice + "\t");
			formatRecordBuffer.append(oStreamID + "\t");
			formatRecordBuffer.append(oEnvIP + "\t");
			formatRecordBuffer.append(oAppURL1 + "\t");
			formatRecordBuffer.append(oAppURL2 + "\t");
			formatRecordBuffer.append(oAppURL3 + "\t");
			formatRecordBuffer.append(tDateID + "\t");
			formatRecordBuffer.append(tHourID + "\t");
			formatRecordBuffer.append(tIP + "\t");
			formatRecordBuffer.append(tProvinceID + "\t");
			formatRecordBuffer.append(tCityID + "\t");
			formatRecordBuffer.append(tIspID);

			return formatRecordBuffer.toString();
		}
	}

	public static class SpreadPreprocessReduce extends Reducer<Text, Text, Text, Text>
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

		Job job = new Job(conf, "SpreadPreprocessMR");
		job.setJarByClass(SpreadPreprocessMR.class);
		job.setInputFormatClass(LzoTextInputFormat.class);

		FileInputFormat.setInputPaths(job, PathProcessor.listHourPaths(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.setMapperClass(SpreadPreprocessMap.class);
		job.setReducerClass(SpreadPreprocessReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	public static void main(String[] argv) throws Exception
	{
		PreprocessMRArgsProcessor argsProcessor = new PreprocessMRArgsProcessor();
		try
		{
			argsProcessor.initDefaultOptions("log_analytics_ios_exchange.jar");
			argsProcessor.parseAndCheckArgs(argv);
		}
		catch (Exception e)
		{
			System.out.println(e.toString());
			argsProcessor.getAutoHelpParser().printMRJarUsage();
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new SpreadPreprocessMR(),
				argsProcessor.getOptionsValueArray());
		System.out.println(nRet);
	}

}
