package com.bi.mobile.playtm.format.correctdata;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bi.mobile.comm.dm.pojo.DMInforHashEnum;
import com.bi.mobile.playtm.format.dataenum.PlayTMOutIHFormatEnum;

public class PlayTMJoinDMIHFormatMR {

	public static class PlayTMJoinDMIHFormatMap extends
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
				// playtm
				else {
					String inforhash = fields[PlayTMOutIHFormatEnum.IH.ordinal()];
					context.write(new Text(inforhash.trim().toLowerCase()),
							new Text(line));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
	}

	public static class PlayTMJoinDMIHFormatReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// System.out.println("*********************************************");
			String inforhashStr = null;
			List<String> playtmInfoList = new ArrayList<String>();
			for (Text val : values) {
				String value = val.toString().trim();
				if (value.contains(",")) {
					inforhashStr = value;
					
				} else {
				
					playtmInfoList.add(value);
				}
			}
			for (String playtmInfoString : playtmInfoList) {
				String playTMETLValue = "";
				String[] splitPlayTMSts = playtmInfoString.split("\t");
	            //System.out.println("splitplay length:"+ splitPlayTMSts.length);
				List<String> splitPlayTMList = new ArrayList<String>();
				for (String splitPlayTM : splitPlayTMSts) {
					splitPlayTMList.add(splitPlayTM.trim());
				}
				if (null != inforhashStr) {
					String[] inforhashStrs = inforhashStr.split(",");
					splitPlayTMList
							.add(PlayTMOutIHFormatEnum.CITY_ID.ordinal(),
									inforhashStrs[DMInforHashEnum.CHANNEL_ID
											.ordinal()]);
					splitPlayTMList.add(
							PlayTMOutIHFormatEnum.PROVINCE_ID.ordinal() + 1,
							inforhashStrs[DMInforHashEnum.MEIDA_ID.ordinal()]);
					splitPlayTMList.add(
							PlayTMOutIHFormatEnum.PROVINCE_ID.ordinal() + 2,
							inforhashStrs[DMInforHashEnum.SERIAL_ID.ordinal()]);

				} else {
				
					splitPlayTMList.add(
							PlayTMOutIHFormatEnum.CITY_ID.ordinal(), "-1");
					splitPlayTMList.add(
							PlayTMOutIHFormatEnum.PROVINCE_ID.ordinal() + 1,
							"-1");
					splitPlayTMList.add(
							PlayTMOutIHFormatEnum.PROVINCE_ID.ordinal() + 2,
							"-1");
					
				}
			//	System.out.println(splitPlayTMList.size());
				for (int i = 0; i < splitPlayTMList.size(); i++) {
					 playTMETLValue +=splitPlayTMList.get(i);
					if (i < splitPlayTMList.size()) {
						playTMETLValue +="\t";
					}
				}
				context.write(new Text(playTMETLValue.trim()),
						new Text());
			}

		}
	}

}
