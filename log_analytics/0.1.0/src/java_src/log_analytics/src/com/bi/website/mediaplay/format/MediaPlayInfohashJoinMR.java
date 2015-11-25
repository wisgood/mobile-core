package com.bi.website.mediaplay.format;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bi.mobile.comm.dm.pojo.DMInforHashEnum;



public class MediaPlayInfohashJoinMR {
	public static class MediaPlayInfoJoinMap extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException, UnsupportedEncodingException {
			try {
				String line = value.toString();
				String[] fields = line.split("\t");

		 if (fields.length == DMInforHashEnum.CHANNEL_ID.ordinal() + 1) {
			String inforhashlinebycomma = line.replaceAll("\t", ",");

			context.write(new Text(fields[DMInforHashEnum.IH.ordinal()]
					.trim().toLowerCase()), new Text(
					inforhashlinebycomma));
		 }
		// MediaPlay
		else {
			String inforhash = fields[MediaPlayEtlEnum.HASHID.ordinal()];
			context.write(new Text(inforhash.trim().toLowerCase()),
					new Text(line));
		}
	   } catch (ArrayIndexOutOfBoundsException e) {
		  e.printStackTrace();
	     }
   }
}

public static class MediaPlayInfoJoinReduce extends
	Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
    	
    	String inforhashStr = null;
    	List<String> MediaPlayInfoList = new ArrayList<String>();
    	for (Text val : values) {
    		String value = val.toString().trim();
    		if (value.contains(",")) {
    			inforhashStr = value;

    		} else {
    			MediaPlayInfoList.add(value);
    		}
    	}
    	for (String MediaPlayInfoString : MediaPlayInfoList) {
    		String MediaPlayETLValue = "";
    		String[] splitMediaPlaySts = MediaPlayInfoString.split("\t");
    		List<String> splitMediaPlayList = new ArrayList<String>();
    		for (String splitMediaPlay : splitMediaPlaySts) {
    			splitMediaPlayList.add(splitMediaPlay);
    		}
    		if (null != inforhashStr) {
    			String[] infohashStrs = inforhashStr.split(",");
    			splitMediaPlayList.add(MediaPlayEtlEnum.TIMESTAMP.ordinal(), 
    					infohashStrs[DMInforHashEnum.SERIAL_ID.ordinal()]);
		 } else {
			splitMediaPlayList.add(MediaPlayEtlEnum.TIMESTAMP.ordinal(), "-1");
		}
		for (int i = 0; i < splitMediaPlayList.size(); i++) {

			MediaPlayETLValue += splitMediaPlayList.get(i);
			if (i < splitMediaPlayList.size()) {
				MediaPlayETLValue += "\t";
			}
			}
		context.write(new Text(MediaPlayETLValue), new Text(""));
	}
  }
 }

}
