package com.bi.website.videoplay.format.mr;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bi.mobile.comm.dm.pojo.DMInforHashEnum;
import com.bi.website.videoplay.format.constant.DMVideoEnum;
import com.bi.website.videoplay.format.constant.WatchVVWebFormatEnum;

public class WatchVVInfohashJoinMR {
	public static class WatchVVInfohashJoinMap extends Mapper<LongWritable, Text, Text, Text> {
		private int typeId = 0;
//		private static Logger logger = Logger.getLogger(WatchVVInfohashJoinMap.class.getName());
	
		public void setup(Context context){
	        // TODO Auto-generated method stub
			String typeStr = context.getConfiguration().get("flags");
			typeId = Integer.parseInt(typeStr);
		}

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException, UnsupportedEncodingException {
			try {
				String line = value.toString();
				String[] fields = line.split("\t");

				if (fields.length <= DMInforHashEnum.CHANNEL_ID.ordinal() + 1) {
					String dimLine = line.replaceAll("\t", ",");

					context.write(new Text(fields[DMInforHashEnum.IH.ordinal()]
							.trim().toLowerCase()), new Text(dimLine));
		        }
				else {
					if(typeId == 1){
						String infohash = fields[WatchVVWebFormatEnum.HASHID.ordinal()];
						context.write(new Text(infohash.trim().toLowerCase()),
								new Text(line));
					}
					//else {
					else if(typeId == 2){
						String videoid = fields[WatchVVWebFormatEnum.VIDEO_ID.ordinal()];
						context.write(new Text(videoid.trim().toLowerCase()),
								new Text(line));
					}
		        }
	          } catch (ArrayIndexOutOfBoundsException e) {
		           e.printStackTrace();
	           }
       }
    }

    public static class WatchVVInfohashJoinReduce extends Reducer<Text, Text, Text, Text> {
		private int typeId = 0;
		
		public void setup(Context context) {
	        // TODO Auto-generated method stub
			String typeStr = context.getConfiguration().get("flags");
			typeId = Integer.parseInt(typeStr);
		}
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {   	
			String dimStr = null;
			List<String> WatchVVInfoList = new ArrayList<String>();
			for (Text val : values) {
				String value = val.toString().trim();
				if (value.contains(",")) {
					dimStr = value;
				} else {
					WatchVVInfoList.add(value);
				}
			}
			for (String WatchVVInfoString : WatchVVInfoList) {
				String WatchVVFormatValue = "";
				String[] splitMediaPlaySts = WatchVVInfoString.split("\t");
				List<String> splitMediaPlayList = new ArrayList<String>();
				for (String splitMediaPlay : splitMediaPlaySts) {
					splitMediaPlayList.add(splitMediaPlay);
				}
				if (null != dimStr) {
					String[] dimStrs = dimStr.split(",");
					if (typeId == 1){
						splitMediaPlayList.add(WatchVVWebFormatEnum.TIMESTAMP.ordinal(), 
								dimStrs[DMInforHashEnum.CHANNEL_ID.ordinal()]);
						splitMediaPlayList.add(WatchVVWebFormatEnum.IP.ordinal(), 
								dimStrs[DMInforHashEnum.SERIAL_ID.ordinal()]);
					}
					else if (typeId == 2){
						splitMediaPlayList.add(WatchVVWebFormatEnum.TIMESTAMP.ordinal(), 
								dimStrs[DMVideoEnum.CHANNEL_ID.ordinal()]);
						splitMediaPlayList.add(WatchVVWebFormatEnum.IP.ordinal(), 
								dimStrs[DMVideoEnum.VIDEO_NAME.ordinal()]);
					}
				} 
				else {
					splitMediaPlayList.add(WatchVVWebFormatEnum.TIMESTAMP.ordinal(), "-1");
					if(typeId == 2){
						splitMediaPlayList.add(WatchVVWebFormatEnum.IP.ordinal(), "unknown");
					}
					else if (typeId == 1){
						splitMediaPlayList.add(WatchVVWebFormatEnum.IP.ordinal(), "-1");	
					}
					
				}
				for (int i = 0; i < splitMediaPlayList.size(); i++) {
					WatchVVFormatValue += splitMediaPlayList.get(i);
					if (i < splitMediaPlayList.size()) {
						WatchVVFormatValue += "\t";
					}
				}
				context.write(new Text(WatchVVFormatValue), new Text(""));
			}
		}
    }
}
