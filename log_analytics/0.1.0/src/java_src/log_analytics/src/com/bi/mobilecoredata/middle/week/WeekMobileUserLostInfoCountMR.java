/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekMobileUserLostInfoCountMR.java 
 * @Package com.bi.mobilecoredata.middle.week 
 * @Description: 周流失数
 * @author fuys
 * @date 2013-7-8 上午10:48:01 
 * @input:输入日志路径/2013-7-8   /dw/logs/mobile/result/week/f_mobile_user_newold_week_info_detail
 * @output:输出日志路径/2013-7-8 /dw/logs/mobile/result/week/f_mobile_user_newold_week_info_detail_count/
 * @executeCmd:hadoop jar log_analytics.jar com.bi.mobilecoredata.middle.week.WeekMobileUserLostInfoCountMR --input /dw/logs/mobile/result/week/f_mobile_user_newold_week_info_detail/$DIR_WEEKDATE --output /dw/logs/mobile/result/week/f_mobile_user_newold_week_info_detail_count/$DIR_WEEKDATE
 * @inputFormat:DATE_ID QUDAO PLAT MAC ISTHISWEEK ISNEW ISNEWOLD ISOLDOLD ISOLDNEW ISTHISWEEKLOST ISNEWLOST ISNEWOLDLOST ISOLDOLDLOST ISOLDNEWLOST ISTHISWEEKLOGINDAYS ISNEWLOGINDAYS ISNEWOLDLOGINDAYS ISOLDOLDLOGINDAYS ISOLDNEWLOGINDAYS
 * @ouputFormat:DATE_ID QUDAO PLAT ISTHISWEEK ISNEW ISNEWOLD ISOLDOLD ISOLDNEW ISTHISWEEKLOST ISNEWLOST ISNEWOLDLOST ISOLDOLDLOST ISOLDNEWLOST ISTHISWEEKLOGINDAYS ISNEWLOGINDAYS ISNEWOLDLOGINDAYS ISOLDOLDLOGINDAYS ISOLDNEWLOGINDAYS
 */
package com.bi.mobilecoredata.middle.week;

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
import com.bi.comm.util.DateFormatInfo;


/**
 * @ClassName: WeekMobileUserLostInfoCountMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-8 上午10:48:01
 */
public class WeekMobileUserLostInfoCountMR extends Configured implements Tool {

    public static class WeekMobileUserLostInfoCountMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        enum WeekMobileUserLostInfoEnum {

            DATE_ID, QUDAO, PLAT, MAC, ISTHISWEEK, ISNEW, ISNEWOLD, ISOLDOLD, ISOLDNEW, ISTHISWEEKLOST, ISNEWLOST, ISNEWOLDLOST, ISOLDOLDLOST, ISOLDNEWLOST, ISTHISWEEKLOGINDAYS, ISNEWLOGINDAYS, ISNEWOLDLOGINDAYS, ISOLDOLDLOGINDAYS, ISOLDNEWLOGINDAYS
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] fields = line.split(DateFormatInfo.SEPARATOR);
            if (fields.length > WeekMobileUserLostInfoEnum.ISOLDNEWLOGINDAYS
                    .ordinal()) {
                String dateId = fields[WeekMobileUserLostInfoEnum.DATE_ID
                        .ordinal()];
                String keyStr = fields[WeekMobileUserLostInfoEnum.DATE_ID
                        .ordinal()]
                        + DateFormatInfo.SEPARATOR
                        + fields[WeekMobileUserLostInfoEnum.QUDAO.ordinal()]
                        + DateFormatInfo.SEPARATOR
                        + fields[WeekMobileUserLostInfoEnum.PLAT.ordinal()];

                context.write(
                        new Text(keyStr),
                        new Text(
                                fields[WeekMobileUserLostInfoEnum.ISTHISWEEK
                                        .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISNEW
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISNEWOLD
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISOLDOLD
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISOLDNEW
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISTHISWEEKLOST
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISNEWLOST
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISNEWOLDLOST
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISOLDOLDLOST
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISOLDNEWLOST
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISTHISWEEKLOGINDAYS
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISNEWLOGINDAYS
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISNEWOLDLOGINDAYS
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISOLDOLDLOGINDAYS
                                                .ordinal()]
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileUserLostInfoEnum.ISOLDNEWLOGINDAYS
                                                .ordinal()]));
            }
        }

    }

    public static class WeekMobileUserLostInfoCountReducer extends
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
                String[] fields = line.split(DateFormatInfo.SEPARATOR);
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
            context.write(key, new Text(currentUserCount + DateFormatInfo.SEPARATOR
                    + currentNewUserCount + DateFormatInfo.SEPARATOR + currentNewOldUserCount
                    + DateFormatInfo.SEPARATOR + currentOldOldUserCount + DateFormatInfo.SEPARATOR
                    + currentOldNewUserCount + DateFormatInfo.SEPARATOR + currentLostUserCount
                    + DateFormatInfo.SEPARATOR + currentNewLostUserCount + DateFormatInfo.SEPARATOR
                    + currentNewOldLostUserCount + DateFormatInfo.SEPARATOR
                    + currentOldOldLostUserCount + DateFormatInfo.SEPARATOR
                    + currentOldNewLostUserCount + DateFormatInfo.SEPARATOR + currentLoginDaysCount
                    + DateFormatInfo.SEPARATOR + currentNewLoginDaysCount + DateFormatInfo.SEPARATOR
                    + currentNewOldLoginDaysCount + DateFormatInfo.SEPARATOR
                    + currentOldOldLoginDaysCount + DateFormatInfo.SEPARATOR
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
        commArgs.init("weekuserlostinfocount.jar");
        commArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new WeekMobileUserLostInfoCountMR(), commArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_weekmobileuserlostinfocountmr");
        job.setJarByClass(WeekMobileUserLostInfoCountMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(WeekMobileUserLostInfoCountMapper.class);
        job.setCombinerClass(WeekMobileUserLostInfoCountReducer.class);
        job.setReducerClass(WeekMobileUserLostInfoCountReducer.class);
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
