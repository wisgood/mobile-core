/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekMobileUserInfoCountMR.java 
 * @Package com.bi.mobilecoredata.middle.week 
 * @Description: 周新老用户数
 * @author fuys
 * @date 2013-7-5 下午4:48:06 
 * @input:输入日志路径/2013-7-5 /dw/logs/mobile/result/week/f_mobile_user_newold_week_info
 * @output:输出日志路径/2013-7-5 /dw/logs/mobile/result/week/f_mobile_user_newold_week_info_count/
 * @executeCmd:hadoop jar log_analytics.jar com.bi.mobilecoredata.middle.week.WeekMobileUserInfoCountMR --input /dw/logs/mobile/result/week/f_mobile_user_newold_week_info/$DIR_WEEKDATE --output /dw/logs/mobile/result/week/f_mobile_user_newold_week_info_count/$DIR_WEEKDATE
 * @inputFormat:DATE_ID QUDAO PLAT MAC  ISTHISWEEK ISNEW ISNEWOLD ISOLDOLD ISOLDNEW LOGINDAYS
 * @ouputFormat:DATE_ID QUDAO PLAT THISWEEKCOUNT NEWCOUNT NEWOLDCOUNT OLDOLDCOUNT OLDNEWCOUNT LOGINDAYS
 */
package com.bi.mobilecoredata.middle.week;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.bi.comm.util.CommArgs;
import com.bi.comm.util.DateFormatInfo;

/**
 * @ClassName: WeekMobileUserInfoCountMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-5 下午4:48:06
 */
public class WeekMobileUserInfoCountMR extends Configured implements Tool {

    public static class WeekMobileUserInfoCountMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        enum WeekInfoEnum {

            DATE_ID, QUDAO, PLAT, MAC, ISTHISWEEK, ISNEW, ISNEWOLD, ISOLDOLD, ISOLDNEW, LOGINDAYS
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] fields = line.split(DateFormatInfo.SEPARATOR);
            if (fields.length > WeekInfoEnum.LOGINDAYS.ordinal()) {
                String keyStr = fields[WeekInfoEnum.DATE_ID.ordinal()]
                        + DateFormatInfo.SEPARATOR
                        + fields[WeekInfoEnum.QUDAO.ordinal()]
                        + DateFormatInfo.SEPARATOR
                        + fields[WeekInfoEnum.PLAT.ordinal()];

                context.write(new Text(keyStr), new Text(
                        fields[WeekInfoEnum.ISTHISWEEK.ordinal()]
                                + DateFormatInfo.SEPARATOR
                                + fields[WeekInfoEnum.ISNEW.ordinal()]
                                + DateFormatInfo.SEPARATOR
                                + fields[WeekInfoEnum.ISNEWOLD.ordinal()]
                                + DateFormatInfo.SEPARATOR
                                + fields[WeekInfoEnum.ISOLDOLD.ordinal()]
                                + DateFormatInfo.SEPARATOR
                                + fields[WeekInfoEnum.ISOLDNEW.ordinal()]
                                + DateFormatInfo.SEPARATOR
                                + fields[WeekInfoEnum.LOGINDAYS.ordinal()]));
            }
        }

    }

    public static class WeekMobileUserInfoCountReducer extends
            Reducer<Text, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(WeekMobileUserInfoCountReducer.class.getName());
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            int weekUserCount = 0;
            int newUserCount = 0;
            int newOldUserCount = 0;
            int oldOldUserCount = 0;
            int oldNewUserCount = 0;
            int logindays = 0;

            for (Text value : values) {
                String line = value.toString().trim();

                String[] fields = line.split(DateFormatInfo.SEPARATOR);
                weekUserCount += Integer.parseInt(fields[0]);
                newUserCount += Integer.parseInt(fields[1]);
                newOldUserCount += Integer.parseInt(fields[2]);
                oldOldUserCount += Integer.parseInt(fields[3]);
                oldNewUserCount += Integer.parseInt(fields[4]);
                logindays += Integer.parseInt(fields[5]);
            }
            context.write(key, new Text(weekUserCount
                    + DateFormatInfo.SEPARATOR + newUserCount
                    + DateFormatInfo.SEPARATOR + newOldUserCount
                    + DateFormatInfo.SEPARATOR + oldOldUserCount
                    + DateFormatInfo.SEPARATOR + oldNewUserCount
                    + DateFormatInfo.SEPARATOR + logindays));

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
                new WeekMobileUserInfoCountMR(), commArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_weekmobileuserinfocountmr");
        job.setJarByClass(WeekMobileUserInfoCountMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(WeekMobileUserInfoCountMapper.class);
        job.setCombinerClass(WeekMobileUserInfoCountReducer.class);
        job.setReducerClass(WeekMobileUserInfoCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
