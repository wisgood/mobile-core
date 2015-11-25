/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: HistoryRemainRateMR.java 
 * @Package com.bi.dingzi.ver2.cal 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-1 下午5:47:42 
 * @input:输入日志路径/2013-8-1
 * @output:输出日志路径/2013-8-1
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.dingzi.ver2.cal;

import java.io.IOException;
import java.math.BigDecimal;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.dingzi.day.user.WeekCountByWeekDayArgs;

/**
 * @ClassName: HistoryRemainRateMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-1 下午5:47:42
 */
public class HistoryRemainRateMR extends Configured implements Tool {

    private static final String HIST_PATH = "history";

    private static final String NEW_PATH = "new";

    public static class HistoryRemainRateMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private String filePathStr = null;

        private String dateStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            this.filePathStr = fileInputSplit.getPath().getParent().toString();
            System.out.println(this.filePathStr);
            this.dateStr = context.getConfiguration().get("date");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String[] fields = value.toString().split("\t");
            String keyStr = null;
            String valueStr = null;
            if (this.filePathStr.contains(HistoryRemainRateMR.HIST_PATH)) {
                keyStr = this.dateStr;
                valueStr = HistoryRemainRateMR.HIST_PATH + fields[1];
            }
            else if (this.filePathStr.contains(HistoryRemainRateMR.NEW_PATH)) {
                keyStr = fields[0];
                valueStr = HistoryRemainRateMR.NEW_PATH + fields[1];
            }
            else {
                keyStr = fields[0];
                valueStr = fields[1];
            }
            context.write(new Text(keyStr), new Text(valueStr));
        }
    }

    public static class HistoryRemainRateReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            double otherUserCount = 0;
            double historyUserCount = 0;
            double newUserCountCount = 0;
            double resultRate = 0;

            for (Text value : values) {

                String valueStr = value.toString();
              
                if (valueStr.contains(HistoryRemainRateMR.HIST_PATH)) {

                    String tmpValue = valueStr
                            .substring(HistoryRemainRateMR.HIST_PATH.length());
                    historyUserCount = Double.parseDouble(tmpValue);
                    System.out.println("historyUserCount:"+historyUserCount);

                }
                else if (valueStr.contains(HistoryRemainRateMR.NEW_PATH)) {
                    String tmpValue = valueStr
                            .substring(HistoryRemainRateMR.NEW_PATH.length());

                    newUserCountCount = Double.parseDouble(tmpValue);
                    System.out.println("newUserCountCount:"+newUserCountCount);
                }
                else {

                    otherUserCount = Double.parseDouble(valueStr);
                    System.out.println("otherUserCount:"+otherUserCount);
                }

            }

            resultRate = (otherUserCount - newUserCountCount)
                    / historyUserCount;
            System.out.println("resultRate:"+resultRate);
            if (resultRate <= 0) {
                resultRate = 0;
            }
            BigDecimal bg = new BigDecimal(resultRate);
            resultRate = bg.setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
            long resultLong = (long) (resultRate * 10000);
            System.out.println("resultLong:"+resultLong);
            context.write(key, new Text(resultLong + ""));

        }

    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        WeekCountByWeekDayArgs dateInfoArgs = new WeekCountByWeekDayArgs();
        dateInfoArgs.init("historyremainrate.jar");
        dateInfoArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new HistoryRemainRateMR(),
                dateInfoArgs.getCountParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "5_product_dingzi_historyremainrate");
        job.setJarByClass(HistoryListUserMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("date", args[2]);
        job.setMapperClass(HistoryRemainRateMapper.class);
        job.setReducerClass(HistoryRemainRateReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
