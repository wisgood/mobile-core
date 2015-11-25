package com.bi.mobile.fbuffer.format.correctdata;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bi.mobile.comm.dm.pojo.DMInforHashEnum;
import com.bi.mobile.fbuffer.format.dataenum.FbufferOutIHFormatEnum;

public class FbufferJoinDMIHFormatMR {

	public static class FbufferJoinDMIHFormatMap extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException,
				UnsupportedEncodingException {
			try {
				String line = value.toString();
				String[] fields = line.split("\t");

				// dm_inforhash
				if (fields.length == DMInforHashEnum.CHANNEL_ID.ordinal() + 1) {
					String inforhashlinebycomma = line.replaceAll("\t", ",");

					context.write(new Text(fields[DMInforHashEnum.IH.ordinal()]
							.trim().toLowerCase()), new Text(
							inforhashlinebycomma));
				}
				// fbuffer
				else {
					String inforhash = fields[FbufferOutIHFormatEnum.IH.ordinal()];
					context.write(new Text(inforhash.trim().toLowerCase()),
							new Text(line));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
	}

	public static class FbufferJoinDMIHFormatReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String inforhashStr = null;
			List<String> fbufferInfoList = new ArrayList<String>();
			for (Text val : values) {
				String value = val.toString().trim();
				if (value.contains(",")) {
					inforhashStr = value;

				} else {

					fbufferInfoList.add(value);
				}
			}
			for (String fbufferInfoString : fbufferInfoList) {
				String fbufferETLValue = "";
				String[] splitDownloadSts = fbufferInfoString.split("\t");
				List<String> splitFbufferList = new ArrayList<String>();
				for (String splitDownload : splitDownloadSts) {

					splitFbufferList.add(splitDownload);
				}

				if (null != inforhashStr) {
					String[] inforhashStrs = inforhashStr.split(",");

					splitFbufferList
							.add(FbufferOutIHFormatEnum.CITY_ID.ordinal(),
									inforhashStrs[DMInforHashEnum.CHANNEL_ID
											.ordinal()]);
					splitFbufferList.add(
							FbufferOutIHFormatEnum.PROVINCE_ID.ordinal() + 1,
							inforhashStrs[DMInforHashEnum.MEIDA_ID.ordinal()]);
					splitFbufferList.add(
							FbufferOutIHFormatEnum.PROVINCE_ID.ordinal() + 2,
							inforhashStrs[DMInforHashEnum.SERIAL_ID.ordinal()]);

				} else {

					splitFbufferList.add(FbufferOutIHFormatEnum.CITY_ID.ordinal(),
							"-1");
					splitFbufferList
							.add(FbufferOutIHFormatEnum.PROVINCE_ID.ordinal() + 1,
									"-1");
					splitFbufferList
							.add(FbufferOutIHFormatEnum.PROVINCE_ID.ordinal() + 2,
									"-1");

				}
				for (int i = 0; i < splitFbufferList.size(); i++) {

					fbufferETLValue += splitFbufferList.get(i);
					if (i < splitFbufferList.size()) {
						fbufferETLValue += "\t";
					}
				}

				context.write(new Text(fbufferETLValue), new Text(""));
			}

		}
	}

}
