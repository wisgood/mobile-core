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
import com.bi.ibidian.datadefine.IbidianClickEnum;
import com.bi.ibidian.dimension.AbstractDimDAO;
import com.bi.ibidian.dimension.DimIPTableDAO;
import com.bi.ibidian.jargsparser.ProprecessMRArgs;
import com.bi.ibidian.util.DecodeStringField;
import com.bi.ibidian.util.DottedDecimalNotation;
import com.bi.ibidian.util.DottedDecimalNotation.DottedDecimalNotationException;
import com.bi.ibidian.util.TimeStamp;

public class IbidianClickPreprocessMR extends Configured implements Tool
{
	public static class IbidianClickPreprocessMap extends Mapper<LongWritable, Text, Text, Text>
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
				//		new Text(originRecord.split(",")[IbidianClickEnum.O_TIMESTAMP.ordinal()]),
				//		new Text(originRecord));
			}
			else
			{
				context.write(
						new Text(formatRecord.split("\t")[IbidianClickEnum.O_TIMESTAMP.ordinal()]),
						new Text(formatRecord));
			}
		}

		private String getFormatRecord(String originRecord)
		{
			String[] splitsOriginRecord = originRecord.trim().split(",");

			// 1. Filter & Clean each field
			String tmpStr = null;
			// 1.1
			long oTimeStamp;
			try
			{
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_TIMESTAMP.ordinal()].trim();
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
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_IP.ordinal()].trim();
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
			String oMAC;
			try
			{
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_MAC.ordinal()].trim();
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
			// 1.4
			int oUserID;
			try
			{
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_USERID.ordinal()].trim();
				if ("".equals(tmpStr))
				{
					oUserID = -1;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oUserID = tmpDouble.intValue();
					}
					catch (NumberFormatException e)
					{
						return CommonEnum.ERROR_RECORD.name();
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oUserID = -1;
			}
			// 1.5
			String oFCK;
			try
			{
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_FCK.ordinal()].trim();
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
			// 1.6
			long oTime;
			try
			{
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_TIME.ordinal()].trim();
				if ("".equals(tmpStr))
				{
					oTime = -1;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oTime = tmpDouble.longValue();
					}
					catch (NumberFormatException e)
					{
						return CommonEnum.ERROR_RECORD.name();
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oTime = -1;
			}
			// 1.7
			String oRefer;
			try
			{
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_REFER.ordinal()].trim();
				try
				{
					oRefer = DecodeStringField.decode(tmpStr, "UTF-8");
					oRefer = oRefer.replaceAll("\\s{1,}", "-");
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oRefer = "UNDEFINED";
			}
			// 1.8
			String oPageName;
			try
			{
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_PAGENAME.ordinal()].trim();
				try
				{
					oPageName = DecodeStringField.decode(tmpStr, "UTF-8");
					oPageName = oPageName.replaceAll("\\s{1,}", "-");
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oPageName = "UNDEFINED";
			}
			// 1.9
			String oBlockName;
			try
			{
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_BLOCKNAME.ordinal()].trim();
				try
				{
					oBlockName = DecodeStringField.decode(tmpStr, "UTF-8");
					oBlockName = oBlockName.replaceAll("\\s{1,}", "-");
				}
				catch (UnsupportedEncodingException e)
				{
					return CommonEnum.ERROR_RECORD.name();
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oBlockName = "UNDEFINED";
			}
			// 1.10
			int oSort;
			try
			{
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_SORT.ordinal()].trim();
				if ("".equals(tmpStr))
				{
					oSort = -1;
				}
				else
				{
					try
					{
						Double tmpDouble = new Double(tmpStr);
						oSort = tmpDouble.intValue();
					}
					catch (NumberFormatException e)
					{
						return CommonEnum.ERROR_RECORD.name();
					}
				}
			}
			catch (IndexOutOfBoundsException e)
			{
				oSort = -1;
			}
			// 1.11
			String oURL;
			try
			{
				tmpStr = splitsOriginRecord[IbidianClickEnum.O_URL.ordinal()].trim();
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
			formatRecordBuffer.append(oMAC + "\t");
			formatRecordBuffer.append(oUserID + "\t");
			formatRecordBuffer.append(oFCK + "\t");
			formatRecordBuffer.append(oTime + "\t");
			formatRecordBuffer.append(oRefer + "\t");
			formatRecordBuffer.append(oPageName + "\t");
			formatRecordBuffer.append(oBlockName + "\t");
			formatRecordBuffer.append(oSort + "\t");
			formatRecordBuffer.append(oURL + "\t");
			formatRecordBuffer.append(tDateID + "\t");
			formatRecordBuffer.append(tHourID + "\t");
			formatRecordBuffer.append(tIP + "\t");
			formatRecordBuffer.append(tProvinceID + "\t");
			formatRecordBuffer.append(tCityID + "\t");
			formatRecordBuffer.append(tIspID);

			return formatRecordBuffer.toString();
		}
	}

	public static class IbidianClickPreprocessReduce extends Reducer<Text, Text, Text, Text>
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
		Job job = new Job(conf, "IbidianClickPreprocessMR");
		job.setJarByClass(IbidianClickPreprocessMR.class);
		FileInputFormat.setInputPaths(job, new Path(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.setMapperClass(IbidianClickPreprocessMap.class);
		job.setReducerClass(IbidianClickPreprocessReduce.class);
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
			mrArgs.initParserOptions("ibidianclickpreprocessmr.jar");
			mrArgs.parseArgs(argv);
		}
		catch (Exception e)
		{
			System.out.println(e.toString());
			mrArgs.getAutoHelpParser().printFuncUsage();
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new IbidianClickPreprocessMR(),
				mrArgs.getOptionValueArray());
		System.out.println(nRet);
	}

}
