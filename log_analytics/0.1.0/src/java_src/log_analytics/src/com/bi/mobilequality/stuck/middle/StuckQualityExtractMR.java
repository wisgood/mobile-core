package com.bi.mobilequality.stuck.middle;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bi.mobilequality.stuck.format.dataenum.StuckFormatEnum;

public class StuckQualityExtractMR
{
	/**
	 * @ClassName: StuckQualityExtractMap
	 * @Description: Static inner classes
	 */
	public static class StuckQualityExtractMap extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String formatRecord = value.toString().trim();
			String[] splitsFormatRecord = formatRecord.split("\t");
			StringBuilder extractRecordbuffer = new StringBuilder();
			long stuckTime = Long.parseLong(splitsFormatRecord[StuckFormatEnum.STUCKTIME.ordinal()]);
			if (stuckTime >= 0 && stuckTime < 2147483647)
			{
				// dimensions
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.DATEID.ordinal()] + "\t");
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.HOURID.ordinal()] + "\t");
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.DEVICEID.ordinal()] + "\t");
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.VERSIONDECIMAL.ordinal()] + "\t");
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.PROVINCEID.ordinal()] + "\t");
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.ISPID.ordinal()] + "\t");
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.SERVERIPDECIMAL.ordinal()] + "\t");
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.NETWORKTYPE.ordinal()] + "\t");
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.BUFFEROK.ordinal()] + "\t");
				// indicators
				extractRecordbuffer.append(stuckTime + "\t");
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.MAC.ordinal()] + "\t");
				extractRecordbuffer.append(splitsFormatRecord[StuckFormatEnum.IPDOTTED.ordinal()]);
				// write out
				context.write(new Text(splitsFormatRecord[StuckFormatEnum.TIMESTAMP.ordinal()]), new Text(extractRecordbuffer.toString()));
			}
		}
	}

	/**
	 * @ClassName: StuckQualityExtractReduce
	 * @Description: Static inner classes
	 */
	public static class StuckQualityExtractReduce extends Reducer<Text, Text, Text, Text>
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

}
