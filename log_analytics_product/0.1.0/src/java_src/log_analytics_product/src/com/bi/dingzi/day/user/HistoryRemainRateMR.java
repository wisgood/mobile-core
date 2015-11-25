/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: HistoryRemainRateMR.java 
 * @Package com.bi.dingzi.day.user 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-5 上午10:58:47 
 */
package com.bi.dingzi.day.user;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @ClassName: HistoryRemainRateMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-5 上午10:58:47
 */
public class HistoryRemainRateMR {

    public static class HistoryRemainRateMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private static final String HIST_PATH = "history_day_user_count";

        private static final String NEW_PATH = "new";

        private String filePathStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            this.filePathStr = fileInputSplit.getPath().toUri().getPath();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String[] fields = value.toString().split("\t");
            String keyStr = null;
            String valueStr = null;
            if (this.filePathStr.contains(HistoryRemainRateMapper.HIST_PATH)) {
                if (fields.length > 1) {

                    try {
                        keyStr = fields[0];
                        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                        Date d = df.parse(keyStr);
                        keyStr = df.format(new Date(d.getTime() + 1 * 24 * 60
                                * 60 * 1000));
                        valueStr = HistoryRemainRateMapper.HIST_PATH
                                + fields[1];
                    }
                    catch(ParseException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

            }
            else if (this.filePathStr
                    .contains(HistoryRemainRateMapper.NEW_PATH)) {
                keyStr = fields[0];
                valueStr = HistoryRemainRateMapper.NEW_PATH + fields[1];

            }
            else {
                keyStr = fields[0];
                valueStr = fields[1];
            }
            context.write(new Text(keyStr), new Text(valueStr));
        }
    }

    public static class HistoryRemainRateReduce extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            // System.out.println("hello reduce");
            double otherUserCount = 0;
            double historyUserCount = 0;
            double newUserCountCount = 0;
            double resultRate = 0;
            // System.out.println(key.toString());
            // System.out.println(values);
            for (Text value : values) {

                String valueStr = value.toString();
                System.out.println(valueStr);
                if (valueStr.contains(HistoryRemainRateMapper.HIST_PATH)) {

                    String tmpValue = valueStr
                            .substring(HistoryRemainRateMapper.HIST_PATH
                                    .length());
                    historyUserCount = Double.parseDouble(tmpValue);
                    // System.out.print("history:" + tmpValue);
                }
                else if (valueStr.contains(HistoryRemainRateMapper.NEW_PATH)) {
                    String tmpValue = valueStr
                            .substring(HistoryRemainRateMapper.NEW_PATH
                                    .length());
                    // System.out.print("new:" + tmpValue);
                    newUserCountCount = Double.parseDouble(tmpValue);
                }
                else {
                    // System.out.print("day:" + valueStr);
                    otherUserCount = Double.parseDouble(valueStr);
                }

            }

            resultRate = (otherUserCount - newUserCountCount)
                    / historyUserCount;
            if (resultRate <= 0) {
                resultRate = 0;
            }

            BigDecimal bg = new BigDecimal(resultRate);
            resultRate = bg.setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
            long resultLong = (long) (resultRate * 10000);

            // try {
            // SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            // Date d = df.parse(key.toString());
            // context.write(
            // new Text(df.format(new Date(d.getTime() + 1 * 24 * 60
            // * 60 * 1000))), new Text(resultLong + ""));
            // }
            // catch(ParseException e) {
            // // TODO Auto-generated catch block
            // e.printStackTrace();
            // }

            context.write(key, new Text(resultLong + ""));

        }

    }
}
