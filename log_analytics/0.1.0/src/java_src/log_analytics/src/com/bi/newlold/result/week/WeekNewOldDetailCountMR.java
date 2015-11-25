package com.bi.newlold.result.week;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.util.CommArgs;

public class WeekNewOldDetailCountMR extends Configured implements Tool {

    enum WeekUserInfoDetailEnum {

        DATE_ID, MAC_CODE, ISTHISWEEK, ISNEW, ISNEWOLD, ISOLDOLD, ISOLDNEW, ISTHISWEEKLOST, ISNEWLOST, ISNEWOLDLOST, ISOLDOLDLOST, ISOLDNEWLOST, ISTHISWEEKLOGINDAYS, ISNEWLOGINDAYS, ISNEWOLDLOGINDAYS, ISOLDOLDLOGINDAYS, ISOLDNEWLOGINDAYS
    }

    public static class WeekNewOldDetailCountMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] fields = line.split("\t");
            if (fields.length > WeekUserInfoDetailEnum.ISOLDNEWLOGINDAYS
                    .ordinal()) {
                String dateId = fields[WeekUserInfoDetailEnum.DATE_ID.ordinal()];

                context.write(
                        new Text(dateId),
                        new Text(
                                fields[WeekUserInfoDetailEnum.ISTHISWEEK
                                        .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISNEW
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISNEWOLD
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISOLDOLD
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISOLDNEW
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISTHISWEEKLOST
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISNEWLOST
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISNEWOLDLOST
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISOLDOLDLOST
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISOLDNEWLOST
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISTHISWEEKLOGINDAYS
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISNEWLOGINDAYS
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISNEWOLDLOGINDAYS
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISOLDOLDLOGINDAYS
                                                .ordinal()]
                                        + "\t"
                                        + fields[WeekUserInfoDetailEnum.ISOLDNEWLOGINDAYS
                                                .ordinal()]));
            }
        }

    }

    public static class WeekNewOldDetailCountReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            // 本周登录用户
            int currentUserCount = 0;
            // 本周登录新用户
            int currentNewUserCount = 0;
            // 本周新老用户
            int currentNewOldUserCount = 0;
            // 本周登录老用户
            int currentOldOldUserCount = 0;
            // 本周老新用户数
            int currentOldNewUserCount = 0;

            // 本周流失用户
            int currentLostUserCount = 0;
            // 本周新用户流失
            int currentNewLostUserCount = 0;
            // 本周新老用户流失
            int currentNewOldLostUserCount = 0;
            // 本周老老用户流失
            int currentOldOldLostUserCount = 0;
            // 本周老新用户流失
            int currentOldNewLostUserCount = 0;

            // 本周用户启动天数
            int currentLoginDaysCount = 0;
            // 本周新用户启动天数
            int currentNewLoginDaysCount = 0;
            // 本周新老用户启动天数
            int currentNewOldLoginDaysCount = 0;
            // 本周老老用户流启动天数
            int currentOldOldLoginDaysCount = 0;
            // 本周老新用户流启动天数
            int currentOldNewLoginDaysCount = 0;
            for (Text value : values) {
                String line = value.toString().trim();
                String[] fields = line.split("\t");
                currentUserCount += Integer.parseInt(fields[0]);
                currentNewUserCount += Integer.parseInt(fields[1]);
                currentNewOldUserCount += Integer.parseInt(fields[2]);
                currentOldOldUserCount += Integer.parseInt(fields[3]);
                currentOldNewUserCount += Integer.parseInt(fields[4]);
                currentLostUserCount += Integer.parseInt(fields[5]);
                currentNewLostUserCount += Integer.parseInt(fields[6]);
                currentNewOldLostUserCount += Integer.parseInt(fields[7]);
                currentOldOldLostUserCount += Integer.parseInt(fields[8]);
                currentOldNewLostUserCount += Integer.parseInt(fields[9]);
                currentLoginDaysCount += Integer.parseInt(fields[10]);
                currentNewLoginDaysCount += Integer.parseInt(fields[11]);
                currentNewOldLoginDaysCount += Integer.parseInt(fields[12]);
                currentOldOldLoginDaysCount += Integer.parseInt(fields[13]);
                currentOldNewLoginDaysCount += Integer.parseInt(fields[14]);
            }
            context.write(key, new Text(currentUserCount + "\t"
                    + currentNewUserCount + "\t" + currentNewOldUserCount
                    + "\t" + currentOldOldUserCount + "\t"
                    + currentOldNewUserCount + "\t" + currentLostUserCount
                    + "\t" + currentNewLostUserCount + "\t"
                    + currentNewOldLostUserCount + "\t"
                    + currentOldOldLostUserCount + "\t"
                    + currentOldNewLostUserCount + "\t" + currentLoginDaysCount
                    + "\t" + currentNewLoginDaysCount + "\t"
                    + currentNewOldLoginDaysCount + "\t"
                    + currentOldOldLoginDaysCount + "\t"
                    + currentOldNewLoginDaysCount));
        }

    }

    /**
     * @throws Exception
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        CommArgs commArgs = new CommArgs();
        commArgs.init("weekusercount.jar");
        commArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new WeekNewOldDetailCountMR(), commArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "WeekNewOldDetailCountMR");
        job.setJarByClass(WeekNewOldDetailCountMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(WeekNewOldDetailCountMapper.class);
        job.setCombinerClass(WeekNewOldDetailCountReducer.class);
        job.setReducerClass(WeekNewOldDetailCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
