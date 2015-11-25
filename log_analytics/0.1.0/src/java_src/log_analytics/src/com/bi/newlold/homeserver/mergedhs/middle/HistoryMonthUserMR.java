package com.bi.newlold.homeserver.mergedhs.middle;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bi.newlold.homeserver.mergedhs.format.MergedhsExtractMR;

/***
 * 
 * @ClassName: HistoryMonthUserMR
 * @Description: process the near three month user
 * @author wang
 * @date 2013-6-20 下午7:34:01
 */

public class HistoryMonthUserMR {

    public enum MonthUserEnum {
        MONTH, MACCODE, TOTALDAY
    }

    public static class HistoryMonthMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        public static final String SEPARATOR = "\t";

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            StringBuilder outputValue = new StringBuilder();
            outputValue.append(fields[MonthUserEnum.MONTH.ordinal()]);
            outputValue.append(SEPARATOR);
            outputValue.append(fields[MonthUserEnum.TOTALDAY.ordinal()]);
            context.write(new Text(fields[MonthUserEnum.MACCODE.ordinal()]),
                    new Text(outputValue.toString()));
        }
    }

    public static class HistoryMonthReducer extends
            Reducer<Text, Text, Text, Text> {

        private int currentMonth;

        private int lastMonth;

        private int previousMonth;

        private int nextMonth;

        private int currentMonthTag;

        private int lastMonthTag;

        private int nextMonthTag;

        private int previousMonthTag;

        private int totalDay;

        public void setup(Context context) throws NumberFormatException {

            try {
                currentMonth = Integer.parseInt(context.getConfiguration().get(
                        "current"));
                lastMonth = Integer.parseInt(context.getConfiguration().get(
                        "last"));
                previousMonth = Integer.parseInt(context.getConfiguration()
                        .get("previous"));
                nextMonth = Integer.parseInt(context.getConfiguration().get(
                        "next"));
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            currentMonthTag = 0;
            lastMonthTag = 0;
            previousMonthTag = 0;
            nextMonthTag = 0;

            for (Text value : values) {
                String[] fields = value.toString().split("\t");
                int month = Integer.parseInt(fields[0]);
                if (month == currentMonth) {
                    currentMonthTag = 1;
                    totalDay = Integer.parseInt(fields[1]);
                }
                else if (month == lastMonth) {
                    lastMonthTag = 1;
                }
                else if (month == previousMonth) {
                    previousMonthTag = 1;
                }
                else if (month == nextMonth) {
                    nextMonthTag = 1;
                }

            }
            StringBuilder output = new StringBuilder();
            output.append(currentMonth + "\t");
            output.append(key.toString() + "\t");
            output.append(totalDay + "\t");
            output.append(currentMonthTag + "\t");
            output.append(lastMonthTag + "\t");
            output.append(previousMonthTag + "\t");
            output.append(nextMonthTag + "\t");
            context.write(new Text(output.toString()), new Text(""));
        }
    }

    /**
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub

        Job job = new Job();
        job.setJarByClass(MergedhsExtractMR.class);
        job.setJobName("HistoryMonthUser-newolduser");
        job.setMapperClass(HistoryMonthMapper.class);
        job.setReducerClass(HistoryMonthReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("test"));
        FileOutputFormat.setOutputPath(job, new Path("output_test"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
