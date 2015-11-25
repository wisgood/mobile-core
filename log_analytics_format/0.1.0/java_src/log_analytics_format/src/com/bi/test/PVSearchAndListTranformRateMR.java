package com.bi.test;

import java.io.IOException;
import java.text.DecimalFormat;

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

import com.bi.common.constant.CommonConstant;
import com.bi.common.logenum.PvFormatEnum;

public class PVSearchAndListTranformRateMR extends Configured implements Tool {

	public static class PVSearchAndListTranformRateMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String mediaKey = "";

			String[] field = value.toString().split("\t", -1);

			String urlFirstId = field[PvFormatEnum.URL_FIRST_ID.ordinal()]
					.trim();

			if (Integer.parseInt(urlFirstId) != 1)
				return;
			String dateId = field[PvFormatEnum.DATE_ID.ordinal()].trim();
			String urlSecondId = field[PvFormatEnum.URL_SECOND_ID.ordinal()]
					.trim();
			String referSecondId = field[PvFormatEnum.REFER_SECOND_ID.ordinal()]
					.trim();
			String referFirstId = field[PvFormatEnum.REFRE_FIRST_ID.ordinal()]
					.trim();
			String fckValue = field[PvFormatEnum.FCK.ordinal()];

			if (urlSecondId.equals("19")) {
				mediaKey = dateId + "\t" + "search";
				fckValue = "searchtab" + field[PvFormatEnum.FCK.ordinal()];
			} else if (urlSecondId.equals("12")) {
				mediaKey = dateId + "\t" + "list";
				fckValue = "listtab" + field[PvFormatEnum.FCK.ordinal()];
			} else if ((urlSecondId.equals("11") || urlSecondId.equals("13"))
						&& referFirstId.equals("1") && referSecondId.equals("19")) {
				mediaKey = dateId + "\t" + "search";
				fckValue = "playfromsb" + field[PvFormatEnum.FCK.ordinal()];
			} else if ((urlSecondId.equals("11") || urlSecondId.equals("13"))
						&& referFirstId.equals("1") && referSecondId.equals("12")) {
				mediaKey = dateId + "\t" + "list";
				fckValue = "playfromlb" + field[PvFormatEnum.FCK.ordinal()];
			}
			if (!mediaKey.isEmpty()) {
				context.write(new Text(mediaKey), new Text(fckValue));
			}

		}
	}

	public static class PVSearchAndListTranformRateReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int searchCount = 0;
			int listCount = 0;
			int playFromSearch = 0;
			int playFromList = 0;
			StringBuilder outValue = new StringBuilder();
			
			for (Text value : values) {
				String val = value.toString();
				if(val.contains("searchtab")){
					searchCount += 1;
				} else if(val.contains("playfromsb")){
					playFromSearch += 1;
				} else if(val.contains("listtab")){
					listCount += 1;
				} else if(val.contains("playfromlb")){
					playFromList += 1;
				}
			}
			String keyStr = key.toString();
			DecimalFormat df = new DecimalFormat("0.000");
			if(keyStr.contains("search")){
				outValue.append(Integer.toString(searchCount) + "\t");
				outValue.append(Integer.toString(playFromSearch) + "\t");
				if(searchCount == 0){
					outValue.append("0.000".toString());
				}
				else{
					outValue.append(df.format((double) playFromSearch / searchCount));
				}
			} else if(keyStr.contains("list")){
				outValue.append(Integer.toString(listCount) + "\t");
				outValue.append(Integer.toString(playFromList) + "\t");
				if(listCount == 0){
					outValue.append("0.000".toString());
				}
				else{
					outValue.append(df.format((double) playFromList / listCount));
				}
			}
			context.write(key, new Text(outValue.toString()));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJarByClass(PVSearchAndListTranformRateMR.class);
		job.setMapperClass(PVSearchAndListTranformRateMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PVSearchAndListTranformRateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		String inputPathStr = job.getConfiguration().get(
				CommonConstant.INPUT_PATH);
		System.out.println(inputPathStr);
		String outputPathStr = job.getConfiguration().get(
				CommonConstant.OUTPUT_PATH);
		int reduceNum = job.getConfiguration().getInt(
				CommonConstant.REDUCE_NUM, 15);
		FileInputFormat.setInputPaths(job, inputPathStr);
		FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
		job.setNumReduceTasks(reduceNum);

		int result = job.waitForCompletion(true) ? 0 : 1;
		return result;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int nRet = ToolRunner.run(new Configuration(),
				new PVSearchAndListTranformRateMR(), args);
		System.out.println(nRet);

	}
}
