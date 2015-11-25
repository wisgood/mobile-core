/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekUserDetailMR.java 
 * @Package com.bi.mobilecoredata.middle.week 
 * @Description: 周明细
 * @author fuys
 * @date 2013-7-5 下午4:48:06 
 * @input:输入日志路径/2013-7-5  一周目录 /dw/logs/mobile/result/daydetail/
 * @output:输出日志路径/2013-7-5 /dw/logs/mobile/result/week/weekdetail/
 * @executeCmd:hadoop jar log_analytics.jar com.bi.mobilecoredata.middle.week.WeekUserDetailMR --input $DIR_COREDATA_DAY_DTAIL_INPUT --output /dw/logs/mobile/result/week/weekdetail/$DIR_WEEKDATE  --dateid  $WEEKDATE
 * @inputFormat:DATE CHANNEL PLAT BOOTUSERSUM NEWUSERSUM PLAYTMSUM
 * @ouputFormat:DATE_ID CHANNEL_ID  PLAT_ID WEEK_BOOT_NUM   ARG_WEEK_BOOT_NUM  WEEK_NEW_BOOT_NUM   ARG_WEEK_NEW_BOOT_NUM   WEEK_PLATM
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName: WeekUserDetailMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-2 下午5:36:47
 */
public class WeekUserDetailMR extends Configured implements Tool {
    private static final String SEPARATOR = "\t";

    private enum Log {
        DATE, CHANNEL, PLAT, BOOTUSERSUM, NEWUSERSUM, PLAYTMSUM
    }

    public static class WeekUserDetailMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            String outputKey = fields[Log.CHANNEL.ordinal()] + SEPARATOR
                    + fields[Log.PLAT.ordinal()];
            String outputValue = fields[Log.DATE.ordinal()] + SEPARATOR
                    + fields[Log.BOOTUSERSUM.ordinal()] + SEPARATOR
                    + fields[Log.NEWUSERSUM.ordinal()] + SEPARATOR
                    + fields[Log.PLAYTMSUM.ordinal()];
            context.write(new Text(outputKey), new Text(outputValue));

        }

    }

    public static class WeekUserDetailReducer extends
            Reducer<Text, Text, Text, Text> {
        private long weekBootUserSum = 0l;

        private long weekNewUserSum = 0l;

        private long weekPlaytmSum = 0l;

        private int daysOfWeek = 7;

        private String firstDayOfWeek = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.firstDayOfWeek = context.getConfiguration().get("dateid");
        }

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            weekBootUserSum = 0;

            weekNewUserSum = 0;

            weekPlaytmSum = 0;
            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                weekBootUserSum += Long.parseLong(fields[1]);
                weekNewUserSum += Long.parseLong(fields[2]);
                weekPlaytmSum += Long.parseLong(fields[3]);

            }
            // o 周日均用户数：本周日用户数的均值
            double avgDayBootUser = weekBootUserSum / daysOfWeek;
            // o 周日均新增用户数：周新增用户数/7
            double avgDayNewUser = weekNewUserSum / daysOfWeek;

            String outputKey = this.firstDayOfWeek + SEPARATOR + key.toString();
            // 周启动天数 周日均启动天数 周新用户启动数 周新用户日均启动天数 周观看时长
            String outputValue = weekBootUserSum + SEPARATOR + avgDayBootUser
                    + SEPARATOR + weekNewUserSum + SEPARATOR + avgDayNewUser
                    + SEPARATOR + weekPlaytmSum;
            // dateid channel plat bootUserSum avgDayBootUser avgDayNewUser
            // playtmSum
            context.write(new Text(outputKey), new Text(outputValue));

        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_weekdetail");
        job.setJarByClass(WeekUserDetailMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("dateid", args[2]);
        job.setMapperClass(WeekUserDetailMapper.class);
        job.setReducerClass(WeekUserDetailReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
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
        WeekUserDetailArgs weekUserDetailArgs = new WeekUserDetailArgs();
        weekUserDetailArgs.init("weekuserdetail.jar");
        weekUserDetailArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new WeekUserDetailMR(),
                weekUserDetailArgs.getCommsParam());
        System.out.println(res);
    }

}
