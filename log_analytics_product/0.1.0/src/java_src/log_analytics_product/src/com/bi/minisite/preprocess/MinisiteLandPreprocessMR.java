package com.bi.minisite.preprocess;

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

import com.bi.minisite.datadefine.CommonEnum;
import com.bi.minisite.datadefine.MinisiteLandEnum;
import com.bi.minisite.dimension.AbstractDimDAO;
import com.bi.minisite.dimension.DimIPTableDAO;
import com.bi.minisite.jargsparser.PreprocessMRArgsProcessor;
import com.bi.minisite.util.CleanField;
import com.bi.minisite.util.CleanField.FieldValueMessyCodeException;
import com.bi.minisite.util.DecodeStringField;
import com.bi.minisite.util.DottedDecimalNotation;
import com.bi.minisite.util.DottedDecimalNotation.DottedDecimalNotationException;
import com.bi.minisite.util.TimeStamp;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class MinisiteLandPreprocessMR extends Configured implements Tool
{
	public static class MinisiteLandPreprocessMap extends Mapper<LongWritable, Text, Text, Text>
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
				//		new Text(originRecord.split(",")[MinisiteLandEnum.O_TIMESTAMP.ordinal()]),
				//		new Text(originRecord));
			}
			else
			{
				context.write(
						new Text(formatRecord.split("\t")[MinisiteLandEnum.O_TIMESTAMP.ordinal()]),
						new Text(formatRecord));
			}
		}

		private String getFormatRecord(String originRecord)
		{
			String[] splitsOriginRecord = originRecord.trim().split(",");

			//1 Filter & Clean each field
			String tmpStr = null;
			//1.1 time_stamp
			long oTimeStamp = 0;
			try
			{
				tmpStr = splitsOriginRecord[MinisiteLandEnum.O_TIMESTAMP.ordinal()].trim();
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
			//1.2 IP
			String oIP;
			try
			{
				tmpStr = splitsOriginRecord[MinisiteLandEnum.O_IP.ordinal()].trim();
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
			//1.3 MAC
			String oMac;
			try
			{
				tmpStr = splitsOriginRecord[MinisiteLandEnum.O_MAC.ordinal()].trim();
				if ("".equals(tmpStr))
				{
					oMac = "UNDEFINED";
				}
				else
				{
					oMac = tmpStr.toUpperCase();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oMac = "UNDEFINED";
			}
			//1.4 tab
			String oTab;
			try
			{
				tmpStr = splitsOriginRecord[MinisiteLandEnum.O_TAB.ordinal()].trim();
				try
				{
					oTab = DecodeStringField.decode(tmpStr, "UTF-8");
					//oTab = oTab.replaceAll("\\s{1,}", "");
					try
					{
						oTab = CleanField.cleanChineseFieldValue(oTab);
					}
					catch (FieldValueMessyCodeException e)
					{
						return CommonEnum.ERROR_RECORD.name();
					}
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oTab = "UNDEFINED";
			}
			//1.5 block
			String oBlock;
			try
			{
				tmpStr = splitsOriginRecord[MinisiteLandEnum.O_BLOCK.ordinal()];
				try
				{
					oBlock = DecodeStringField.decode(tmpStr, "UTF-8");
					//oBlock = oBlock.replaceAll("\\s{1,}", "-");
					try
					{
						oBlock = CleanField.cleanChineseFieldValue(oBlock);
					}
					catch (FieldValueMessyCodeException e)
					{
						return CommonEnum.ERROR_RECORD.name();
					}
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oBlock = "UNDEFINED";
			}
			// 1.6 title
			String oTitle;
			try
			{
				tmpStr = splitsOriginRecord[MinisiteLandEnum.O_TITLE.ordinal()].trim();
				try
				{
					oTitle = DecodeStringField.decode(tmpStr, "UTF-8");
					oTitle = oTitle.replaceAll("\\s{1,}", "");
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oTitle = "UNDEFINED";
			}

			// 1.7 version
			String oVersion;
			try
			{
				tmpStr = splitsOriginRecord[MinisiteLandEnum.O_VERSION.ordinal()];
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

			// 1.8 fck
			String oFCK;
			try
			{
				tmpStr = splitsOriginRecord[MinisiteLandEnum.O_FCK.ordinal()];
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
			formatRecordBuffer.append(oTab + "\t");
			formatRecordBuffer.append(oBlock + "\t");
			formatRecordBuffer.append(oTitle + "\t");
			formatRecordBuffer.append(oVersion + "\t");
			formatRecordBuffer.append(oFCK + "\t");
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

	public static class MinisiteLandPreprocessReduce extends Reducer<Text, Text, Text, Text>
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

		Job job = new Job(conf, "MinisiteLandPreprocessMR");
		job.setJarByClass(MinisiteLandPreprocessMR.class);
		job.setInputFormatClass(LzoTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.setMapperClass(MinisiteLandPreprocessMap.class);
		job.setReducerClass(MinisiteLandPreprocessReduce.class);

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
			argsProcessor.initDefaultOptions("log_analytics_product.jar");
			argsProcessor.parseAndCheckArgs(argv);
		}
		catch (Exception e)
		{
			System.out.println(e.toString());
			argsProcessor.getAutoHelpParser().printMRJarUsage();
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new MinisiteLandPreprocessMR(),
				argsProcessor.getOptionValueArray());
		System.out.println(nRet);
	}

}
