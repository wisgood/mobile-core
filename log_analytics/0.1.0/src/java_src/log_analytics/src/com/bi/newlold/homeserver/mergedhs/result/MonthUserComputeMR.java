package com.bi.newlold.homeserver.mergedhs.result;

/***
 * 
 * input :date,mac,logindays,currenttag,lasttag,previoustag,nexttag
 * output:日期，当月新用户数；当月老用户数；上月新用户数，上月老用户数；当月总用户数；当月总登录天数；上月流失数，上月流失率
 * 
 */
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MonthUserComputeMR {

    public enum HistoryUserEnum {
        DATE, MACCODE, TOTALDAY, CURRENT, LAST, PREVIOUS, NEXT
    }

    public static class MonthUserComputeMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static final String SEPARATOR = "\t";

        private String date;

        private long currentMonthLoginDaySum;

        private long currentMonthTotalUserSum;

        private long currentMonthLostUserSum;

        private long currentMonthNewUserSum;

        private long currentMonthNewUserLostSum;

        private long currentMonthNewUserLoginDaySum;

        private long currentMonthOldUserSum;

        private long currentMonthOldUserLostSum;

        private long currentMonthOldUserLoginDaySum;

        private long lastMonthOldUserSum;

        private long lastMonthOldUserLostSum;

        private long lastMonthOldUserLoginDaySum;

        private long lastMonthNewUserSum;

        private long lastMonthNewUserLostSum;

        private long lastMonthNewUserLoginDaySum;

        private long lastMonthTotalUserSum;

        private long lastMonthLostUserSum;

        // private long lastMonthLostLoginDaySum;

        private int loginDays;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            date = fields[HistoryUserEnum.DATE.ordinal()];
            String loginDay = fields[HistoryUserEnum.TOTALDAY.ordinal()];
            int currentTag = Integer.parseInt(fields[HistoryUserEnum.CURRENT
                    .ordinal()]);
            int lastTag = Integer.parseInt(fields[HistoryUserEnum.LAST
                    .ordinal()]);
            int previousTag = Integer.parseInt(fields[HistoryUserEnum.PREVIOUS
                    .ordinal()]);
            int nextTag = Integer.parseInt(fields[HistoryUserEnum.NEXT
                    .ordinal()]);
            loginDays = Integer.parseInt(loginDay);
            int tag = currentTag * 8 + lastTag * 4 + previousTag * 2 + nextTag;
            switch (tag) {
            case 4:
            case 5:
            case 6:
            case 7: {
                lastMonthTotalUserSum++;
                lastMonthLostUserSum++;
                break;
            }
            case 8: {
                addCurrentNew(true);
                break;
            }
            case 9: {
                addCurrentNew(false);
                break;
            }
            case 10: {
                addCurrentNew(true);
                break;

            }
            case 11: {
                addCurrentNew(false);
                break;
            }
            case 12: {
                addCurrentOld(true);
                addLastNew(true);
                break;

            }
            case 13: {
                addCurrentOld(false);
                addLastNew(false);
                break;
            }
            case 14: {
                addCurrentOld(true);
                addLastOld(true);
                break;
            }
            case 15: {
                addCurrentOld(false);
                addLastOld(false);
                break;
            }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            StringBuilder outputValue = new StringBuilder();

            outputValue.append(currentMonthTotalUserSum + "\t"
                    + currentMonthLoginDaySum + "\t" + currentMonthLostUserSum
                    + "\t");
            outputValue.append(currentMonthNewUserSum + "\t"
                    + currentMonthNewUserLoginDaySum + "\t"
                    + currentMonthNewUserLostSum + "\t");
            outputValue.append(currentMonthOldUserSum + "\t"
                    + currentMonthOldUserLoginDaySum + "\t"
                    + currentMonthOldUserLostSum + "\t");
            outputValue.append(lastMonthTotalUserSum + "\t"
                    + lastMonthLostUserSum + "\t");
            outputValue.append(lastMonthNewUserSum + "\t"
                    + lastMonthNewUserLoginDaySum + "\t"
                    + lastMonthNewUserLostSum + "\t");
            outputValue.append(lastMonthOldUserSum + "\t"
                    + lastMonthOldUserLoginDaySum + "\t"
                    + lastMonthOldUserLostSum + "\t");

            context.write(new Text(date), new Text(outputValue.toString()));

            // context.write(new Text(date), new Text(currentMonthNewUserSum
            // + "\t" + currentMonthOldUserSum + "\t"
            // + lastMonthNewUserSum + "\t" + lastMonthOldUserSum + "\t"
            // + currentMonthTotalUserSum + "\t" + currentMonthLoginDaySum
            // + "\t" + lastMonthLostUserSum + "\t"
            // + lastMonthTotalUserSum));
        }

        private void addCurrentNew(boolean lost) {
            // 增加当月用户数和当月老用户数
            currentMonthTotalUserSum++;
            currentMonthLoginDaySum += loginDays;
            currentMonthNewUserLoginDaySum += loginDays;
            currentMonthNewUserSum++;
            if (lost) {
                // 当月新且流失
                currentMonthNewUserLostSum++;
                // 当月用户数且流失
                currentMonthLostUserSum++;
            }

        }

        private void addCurrentOld(boolean lost) {
            // 增加当月用户数和当月老用户数
            currentMonthTotalUserSum++;
            currentMonthLoginDaySum += loginDays;
            currentMonthOldUserLoginDaySum += loginDays;
            currentMonthOldUserSum++;
            if (lost) {
                // 当月老且流失
                currentMonthOldUserLostSum++;
                // 当月用户数且流失
                currentMonthLostUserSum++;
            }

        }

        private void addLastNew(boolean lost) {
            // 增加上月用户数和上月新用户数
            lastMonthTotalUserSum++;
            lastMonthNewUserSum++;
            lastMonthNewUserLoginDaySum += loginDays;
            if (lost) {
                // 上月新且流失
                lastMonthNewUserLostSum++;
                // 上月用户数且流失
//                lastMonthLostUserSum++;
            }

        }

        private void addLastOld(boolean lost) {
            // 增加上月用户数和上月老用户数
            lastMonthTotalUserSum++;
            lastMonthOldUserSum++;
            lastMonthOldUserLoginDaySum += loginDays;
            if (lost) {
                // 上月老且流失
                lastMonthOldUserLostSum++;
                // 上月用户数且流失
//                lastMonthLostUserSum++;
            }

        }

    }

    public static class MonthUserComputeReducer extends
            Reducer<Text, Text, Text, Text> {

        private static final String SEPARATOR = "\t";

        private String date;

        private long currentMonthTotalUserSum;

        private long currentMonthLoginDaySum;

        private long currentMonthLostUserSum;

        private long currentMonthNewUserSum;

        private long currentMonthNewUserLoginDaySum;

        private long currentMonthNewUserLostSum;

        private long currentMonthOldUserSum;

        private long currentMonthOldUserLoginDaySum;

        private long currentMonthOldUserLostSum;

        private long lastMonthTotalUserSum;

        private long lastMonthLostUserSum;

        private long lastMonthNewUserSum;

        private long lastMonthNewUserLoginDaySum;

        private long lastMonthNewUserLostSum;

        private long lastMonthOldUserSum;

        private long lastMonthOldUserLoginDaySum;

        private long lastMonthOldUserLostSum;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                date = key.toString();
                String line = value.toString();
                String[] fields = line.split(SEPARATOR);
                currentMonthTotalUserSum += Integer.parseInt(fields[0]);
                currentMonthLoginDaySum += Integer.parseInt(fields[1]);
                currentMonthLostUserSum += Integer.parseInt(fields[2]);
                currentMonthNewUserSum += Integer.parseInt(fields[3]);
                currentMonthNewUserLoginDaySum += Integer.parseInt(fields[4]);
                currentMonthNewUserLostSum += Integer.parseInt(fields[5]);
                currentMonthOldUserSum += Integer.parseInt(fields[6]);
                currentMonthOldUserLoginDaySum += Integer.parseInt(fields[7]);
                currentMonthOldUserLostSum += Integer.parseInt(fields[8]);
                lastMonthTotalUserSum += Integer.parseInt(fields[9]);
                lastMonthLostUserSum += Integer.parseInt(fields[10]);
                lastMonthNewUserSum += Integer.parseInt(fields[11]);
                lastMonthNewUserLoginDaySum += Integer.parseInt(fields[12]);
                lastMonthNewUserLostSum += Integer.parseInt(fields[13]);
                lastMonthOldUserSum += Integer.parseInt(fields[14]);
                lastMonthOldUserLoginDaySum += Integer.parseInt(fields[15]);
                lastMonthOldUserLostSum += Integer.parseInt(fields[16]);

            }
            StringBuilder outputValue = new StringBuilder();

            outputValue.append(currentMonthTotalUserSum + "\t"
                    + currentMonthLoginDaySum + "\t" + currentMonthLostUserSum
                    + "\t");
            outputValue.append(currentMonthNewUserSum + "\t"
                    + currentMonthNewUserLoginDaySum + "\t"
                    + currentMonthNewUserLostSum + "\t");
            outputValue.append(currentMonthOldUserSum + "\t"
                    + currentMonthOldUserLoginDaySum + "\t"
                    + currentMonthOldUserLostSum + "\t");
            outputValue.append(lastMonthTotalUserSum + "\t"
                    + lastMonthLostUserSum + "\t");
            outputValue.append(lastMonthNewUserSum + "\t"
                    + lastMonthNewUserLoginDaySum + "\t"
                    + lastMonthNewUserLostSum + "\t");
            outputValue.append(lastMonthOldUserSum + "\t"
                    + lastMonthOldUserLoginDaySum + "\t"
                    + lastMonthOldUserLostSum + "\t");

            context.write(new Text(date), new Text(outputValue.toString()));

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
        job.setJarByClass(MonthUserComputeMR.class);
        job.setJobName("MonthUserCompute-newolduser");
        job.setMapperClass(MonthUserComputeMapper.class);
        job.setReducerClass(MonthUserComputeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("dw/result/2013/05"));
        FileOutputFormat.setOutputPath(job, new Path("output_test11"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
/**
 * 
 * 
 * if (EXSIT.equals(currentTag)) { // 当月总用户数 当用总登录天数
 * 
 * currentMonthLoginDaySum += loginDays; currentMonthTotalUserSum++; if
 * (EXSIT.equals(lastTag)) { currentMonthOldUserSum++;
 * currentMonthOldUserLoginDaySum += loginDays; lastMonthTotalUserSum++;
 * lastMonthLostLoginDaySum += loginDays; if (EXSIT.equals(previousTag)) {
 * lastMonthOldUserSum++; lastMonthOldUserLoginDaySum += loginDays; } else {
 * lastMonthNewUserSum++;
 * 
 * } } else { currentMonthNewUserSum++; } } else { if (EXSIT.equals(lastTag)) {
 * lastMonthLostUserSum++; lastMonthTotalUserSum++; }
 * 
 * }
 */
// ///////
