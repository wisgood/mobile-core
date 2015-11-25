package com.bi.ibidian.preprocess;

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

import com.bi.ibidian.datadefine.CommonEnum;
import com.bi.ibidian.datadefine.HotsSpeedEnum;
import com.bi.ibidian.dimension.AbstractDimDAO;
import com.bi.ibidian.dimension.DimIPTableDAO;
import com.bi.ibidian.jargsparser.ProprecessMRArgs;
import com.bi.ibidian.util.DecodeStringField;
import com.bi.ibidian.util.DottedDecimalNotation;
import com.bi.ibidian.util.DottedDecimalNotation.DottedDecimalNotationException;
import com.bi.ibidian.util.TimeStamp;

public class HotsSpeedPreprocessMR extends Configured implements Tool
{

	public static class HotsSpeedPreprocessMap extends Mapper<LongWritable, Text, Text, Text>
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
				//		new Text(originRecord.split(",")[HotsSpeedEnum.O_TIMESTAMP.ordinal()]),
				//		new Text(originRecord));
			}
			else
			{
				context.write(new Text(
						formatRecord.split("\t")[HotsSpeedEnum.O_TIMESTAMP.ordinal()]), new Text(
						formatRecord));
			}
		}

		private String getFormatRecord(String originRecord)
		{
			String[] splitsOriginRecord = originRecord.split(",");

			// 1. Filter & Clean each field
			String tmpStr = null;
			// 1.1
			long oTimeStamp;
			try
			{
				tmpStr = splitsOriginRecord[HotsSpeedEnum.O_TIMESTAMP.ordinal()].trim();
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
			// 1.2
			String oIP;
			try
			{
				tmpStr = splitsOriginRecord[HotsSpeedEnum.O_IP.ordinal()].trim();
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
			// 1.3
			String oFCK;
			try
			{
				tmpStr = splitsOriginRecord[HotsSpeedEnum.O_FCK.ordinal()].trim();
				if ("".equals(tmpStr))
				{
					oFCK = "UNDEFINED";
				}
				else
				{
					oFCK = tmpStr.toUpperCase();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oFCK = "UNDEFINED";
			}
			// 1.4
			String oMAC;
			try
			{
				tmpStr = splitsOriginRecord[HotsSpeedEnum.O_MAC.ordinal()].trim();
				if ("".equals(tmpStr))
				{
					oMAC = "UNDEFINED";
				}
				else
				{
					oMAC = tmpStr.toUpperCase();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oMAC = "UNDEFINED";
			}
			// 1.5
			String oFPC;
			try
			{
				tmpStr = splitsOriginRecord[HotsSpeedEnum.O_FPC.ordinal()].trim();
				if ("".equals(tmpStr))
				{
					oFPC = "UNDEFINED";
				}
				else
				{
					oFPC = tmpStr.toUpperCase();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oFPC = "UNDEFINED";
			}
			// 1.6
			String oURL;
			try
			{
				tmpStr = splitsOriginRecord[HotsSpeedEnum.O_URL.ordinal()].trim();
				try
				{
					oURL = DecodeStringField.decode(tmpStr, "UTF-8");
					oURL = oURL.replaceAll("\\s{1,}", "-");
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oURL = "UNDEFINED";
			}
			// 1.7
			long oVTIME;
			try
			{
				tmpStr = splitsOriginRecord[HotsSpeedEnum.O_VTIME.ordinal()].trim();
				if ("".equals(tmpStr))
				{
					oVTIME = -1;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oVTIME = tmpDouble.longValue();
					}
					catch (NumberFormatException e)
					{
						return CommonEnum.ERROR_RECORD.name();
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oVTIME = -1;
			}

			// 2. Transform from Cleaned fields
			// 2.1
			int tDateID = TimeStamp.getDateID(String.valueOf(oTimeStamp));
			int tHourID = TimeStamp.getHourID(String.valueOf(oTimeStamp));
			// 2.2
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
			formatRecordBuffer.append(oFCK + "\t");
			formatRecordBuffer.append(oMAC + "\t");
			formatRecordBuffer.append(oFPC + "\t");
			formatRecordBuffer.append(oURL + "\t");
			formatRecordBuffer.append(oVTIME + "\t");
			formatRecordBuffer.append(tDateID + "\t");
			formatRecordBuffer.append(tHourID + "\t");
			formatRecordBuffer.append(tIP + "\t");
			formatRecordBuffer.append(tProvinceID + "\t");
			formatRecordBuffer.append(tCityID + "\t");
			formatRecordBuffer.append(tIspID);

			return formatRecordBuffer.toString();
		}
	}

	public static class HotsSpeedPreprocessReduce extends Reducer<Text, Text, Text, Text>
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
		//conf.set("mapred.queue.name", "test");
		Job job = new Job(conf, "HotsSpeedPreprocessMR");
		job.setJarByClass(HotsSpeedPreprocessMR.class);
		FileInputFormat.setInputPaths(job, new Path(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.setMapperClass(HotsSpeedPreprocessMap.class);
		job.setReducerClass(HotsSpeedPreprocessReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] argv) throws Exception
	{
		ProprecessMRArgs mrArgs = new ProprecessMRArgs();
		try
		{
			mrArgs.initParserOptions("hotsspeedpreprocessmr.jar");
			mrArgs.parseArgs(argv);
		}
		catch (Exception e)
		{
			System.out.println(e.toString());
			mrArgs.getAutoHelpParser().printFuncUsage();
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new HotsSpeedPreprocessMR(),
				mrArgs.getOptionValueArray());
		System.out.println(nRet);
	}

}