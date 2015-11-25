/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekMobileUserLostInfoMR.java 
 * @Package com.bi.mobilecoredata.middle.week 
 * @Description: 周流失列表
 * @author fuys
 * @date 2013-7-8 上午9:57:26 
 * @input:输入日志路径/2013-7-8 后四周的/dw/logs/mobile/result/week/weekmac/和当周的/dw/logs/mobile/result/week/f_mobile_user_newold_week_info/
 * @output:输出日志路径/2013-7-8 /dw/logs/mobile/result/week/f_mobile_user_newold_week_info_detail
 * @executeCmd:hadoop jar log_analytics.jar com.bi.mobilecoredata.middle.week.WeekMobileUserLostInfoMR  --input $DIR_ORIGINDATA_WEEKINFODETAIL_INPUT  --output /dw/logs/mobile/result/week/f_mobile_user_newold_week_info_detail/$DIR_BEGINWEEK  --weeksday $PARS_WEEKDATE_IDS
 * @inputFormat:DATE_ID QUDAO PLAT MAC  ISTHISWEEK ISNEW ISNEWOLD ISOLDOLD ISOLDNEW LOGINDAYS
 *              DATE_ID QUDAO PLAT MAC  LOGINDAYS
 * @ouputFormat:DATE_ID QUDAO PLAT MAC ISTHISWEEK ISNEW ISNEWOLD ISOLDOLD ISOLDNEW ISTHISWEEKLOST ISNEWLOST ISNEWOLDLOST ISOLDOLDLOST ISOLDNEWLOST ISTHISWEEKLOGINDAYS ISNEWLOGINDAYS ISNEWOLDLOGINDAYS ISOLDOLDLOGINDAYS ISOLDNEWLOGINDAYS
 */
package com.bi.mobilecoredata.middle.week;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.bi.comm.util.DateFormat;
import com.bi.comm.util.DateFormatInfo;
import com.bi.newlold.middle.weekinfo.WeekInfoArgs;

/**
 * @ClassName: WeekUserLostInfoMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-8 上午9:57:26
 */
public class WeekMobileUserLostInfoMR extends Configured implements Tool {

    public static final String CURRENT_USER = "currentuser";

    public static final String NEW_USER = "newuser";

    public static final String NEW_OLD_USER = "newolduser";

    public static final String OLD_OLD_USER = "oldolduser";

    public static final String OLD_NEW_USER = "oldnewuser";

    enum WeekMobileInfoEnum {

        DATE_ID, QUDAO, PLAT, MAC, ISTHISWEEK, ISNEW, ISNEWOLD, ISOLDOLD, ISOLDNEW, LOGINDAYS
    }

    enum WeekMobileMacEnum {
        DATE_ID, QUDAO, PLAT, MAC, LOGINDAYS
    }

    public static class WeekMobileUserLostInfoMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(WeekMobileUserLostInfoMapper.class.getName());

        private String filePathStr = null;

        public String weekDateid = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            this.filePathStr = fileSplit.getPath().getParent().toString();
            logger.info("filePathStr:" + this.filePathStr);
            this.weekDateid = DateFormat.getDateIdFormPath(this.filePathStr);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] fields = line.split(DateFormatInfo.SEPARATOR);

            if (fields.length > WeekMobileMacEnum.MAC.ordinal()) {
                String keyStr = fields[1] + DateFormatInfo.SEPARATOR
                        + fields[2] + DateFormatInfo.SEPARATOR + fields[3];
                if (this.filePathStr.trim().contains(
                        "f_mobile_user_newold_week_info")) {

                    if ("1".equalsIgnoreCase(fields[WeekMobileInfoEnum.ISTHISWEEK
                            .ordinal()])) {
                        context.write(
                                new Text(keyStr),
                                new Text(this.weekDateid
                                        + DateFormatInfo.SEPARATOR
                                        + WeekMobileUserLostInfoMR.CURRENT_USER
                                        + DateFormatInfo.SEPARATOR
                                        + fields[WeekMobileInfoEnum.LOGINDAYS
                                                .ordinal()]));
                    }
                    if ("1".equalsIgnoreCase(fields[WeekMobileInfoEnum.ISNEW
                            .ordinal()])) {
                        context.write(new Text(keyStr), new Text(
                                this.weekDateid + DateFormatInfo.SEPARATOR
                                        + WeekMobileUserLostInfoMR.NEW_USER));
                    }
                    if ("1".equalsIgnoreCase(fields[WeekMobileInfoEnum.ISNEWOLD
                            .ordinal()])) {
                        context.write(
                                new Text(keyStr),
                                new Text(this.weekDateid + DateFormatInfo.SEPARATOR
                                        + WeekMobileUserLostInfoMR.NEW_OLD_USER));
                    }
                    if ("1".equalsIgnoreCase(fields[WeekMobileInfoEnum.ISOLDOLD
                            .ordinal()])) {
                        context.write(
                                new Text(keyStr),
                                new Text(this.weekDateid + DateFormatInfo.SEPARATOR
                                        + WeekMobileUserLostInfoMR.OLD_OLD_USER));
                    }
                    if ("1".equalsIgnoreCase(fields[WeekMobileInfoEnum.ISOLDNEW
                            .ordinal()])) {
                        context.write(
                                new Text(keyStr),
                                new Text(this.weekDateid + DateFormatInfo.SEPARATOR
                                        + WeekMobileUserLostInfoMR.OLD_NEW_USER));
                    }
                }
                else {
                    if (fields.length > WeekMobileMacEnum.LOGINDAYS.ordinal()) {
                        context.write(new Text(keyStr), new Text(
                                this.weekDateid));
                    }

                }
            }
        }

    }

    public static class WeekMobileUserLostInfoReducer extends
            Reducer<Text, Text, IntWritable, Text> {

        private static Logger logger = Logger
                .getLogger(WeekMobileUserLostInfoReducer.class.getName());

        private Integer currentWeekDate = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            String weeksdayStr = context.getConfiguration().get("weeksday");
            logger.info("weeksdayStr:" + weeksdayStr);
            String[] weeksdayStrs = weeksdayStr.split(",");
            Integer[] weeksdayItgs = new Integer[weeksdayStrs.length];
            for (int i = 0; i < weeksdayStrs.length; i++) {
                weeksdayItgs[i] = new Integer(weeksdayStrs[i]);
            }
            Arrays.sort(weeksdayItgs, new Comparator<Integer>() {
                @Override
                public int compare(Integer first, Integer sercond) {
                    return first.compareTo(sercond);
                }
            });
            this.currentWeekDate = weeksdayItgs[0];
            logger.info("currentweekdate: " + this.currentWeekDate);

        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String keyInfoStr = key.toString();
            // 本周登录用户
            int currentUser = 0;
            // 本周登录新用户
            int currentNewUser = 0;
            // 本周新老用户
            int currentNewOldUser = 0;
            // 本周登录老用户
            int currentOldOldUser = 0;
            // 本周老新用户数
            int currentOldNewUser = 0;

            // 本周流失用户
            int currentLostUser = 0;
            // 本周新用户流失
            int currentNewLostUser = 0;
            // 本周新老用户流失
            int currentNewOldLostUser = 0;
            // 本周老老用户流失
            int currentOldOldLostUser = 0;
            // 本周老新用户流失
            int currentOldNewLostUser = 0;

            // 本周用户启动天数
            int currentLoginDays = 0;
            // 本周新用户启动天数
            int currentNewLoginDays = 0;
            // 本周新老用户启动天数
            int currentNewOldLoginDays = 0;
            // 本周老老用户流启动天数
            int currentOldOldLoginDays = 0;
            // 本周老新用户流启动天数
            int currentOldNewLoginDays = 0;

            List<Integer> loginDateList = new ArrayList<Integer>(20);
            // 本周登录天数
            Map<String, Integer> mapKeyToLoginDayMap = new HashMap<String, Integer>();
            // 本周新用户
            Map<String, Boolean> mapKeyToNewMap = new HashMap<String, Boolean>();
            // 本周新老用户
            Map<String, Boolean> mapKeyToNewOldMap = new HashMap<String, Boolean>();
            // 本周老老用户
            Map<String, Boolean> mapKeyToOldOldMap = new HashMap<String, Boolean>();
            // 本周老新用户
            Map<String, Boolean> mapKeyToOldNewMap = new HashMap<String, Boolean>();
            for (Text value : values) {
                String line = value.toString();
                String[] fields = line.split(DateFormatInfo.SEPARATOR);
                Integer dateId = Integer.parseInt(fields[0]);
                loginDateList.add(dateId);
                if (fields.length > 1) {
                    String loginDateStr = fields[1];
                    if (loginDateStr
                            .equalsIgnoreCase(WeekMobileUserLostInfoMR.NEW_USER)) {
                        mapKeyToNewMap.put(keyInfoStr, new Boolean(true));
                    }

                    if (loginDateStr
                            .equalsIgnoreCase(WeekMobileUserLostInfoMR.NEW_OLD_USER)) {
                        mapKeyToNewOldMap.put(keyInfoStr, new Boolean(true));
                    }
                    if (loginDateStr
                            .equalsIgnoreCase(WeekMobileUserLostInfoMR.OLD_OLD_USER)) {
                        mapKeyToOldOldMap.put(keyInfoStr, new Boolean(true));
                    }
                    if (loginDateStr
                            .equalsIgnoreCase(WeekMobileUserLostInfoMR.OLD_NEW_USER)) {
                        mapKeyToOldNewMap.put(keyInfoStr, new Boolean(true));
                    }

                    if (fields.length == 3) {
                        currentLoginDays = Integer.parseInt(fields[2]);

                    }
                    mapKeyToLoginDayMap.put(keyInfoStr, new Integer(
                            currentLoginDays));
                }

            }
            Integer[] loginDates = new Integer[loginDateList.size()];
            loginDates = loginDateList.toArray(loginDates);
            Arrays.sort(loginDates, new Comparator<Integer>() {
                @Override
                public int compare(Integer first, Integer sercond) {
                    return first.compareTo(sercond);
                }
            });
            // logger.info(Arrays.asList(loginDates));
            if (loginDates[0].equals(this.currentWeekDate)) {
                currentUser = 1;
                currentLostUser = this.getLostCount(loginDates,
                        this.currentWeekDate);
                if (null != mapKeyToNewMap.get(keyInfoStr)
                        && mapKeyToNewMap.get(keyInfoStr).booleanValue()) {
                    currentNewUser = 1;
                    currentNewLostUser = this.getLostCount(loginDates,
                            this.currentWeekDate);
                    currentNewLoginDays = mapKeyToLoginDayMap.get(keyInfoStr)
                            .intValue();

                }
                if (null != mapKeyToNewOldMap.get(keyInfoStr)
                        && mapKeyToNewOldMap.get(keyInfoStr).booleanValue()) {
                    currentNewOldUser = 1;
                    currentNewOldLostUser = this.getLostCount(loginDates,
                            this.currentWeekDate);
                    currentNewOldLoginDays = mapKeyToLoginDayMap
                            .get(keyInfoStr).intValue();

                }
                if (null != mapKeyToOldOldMap.get(keyInfoStr)
                        && mapKeyToOldOldMap.get(keyInfoStr).booleanValue()) {
                    currentOldOldUser = 1;
                    currentOldOldLostUser = this.getLostCount(loginDates,
                            this.currentWeekDate);
                    currentOldOldLoginDays = mapKeyToLoginDayMap
                            .get(keyInfoStr).intValue();

                }
                if (null != mapKeyToOldNewMap.get(keyInfoStr)
                        && mapKeyToOldNewMap.get(keyInfoStr).booleanValue()) {
                    currentOldNewUser = 1;
                    currentOldNewLostUser = this.getLostCount(loginDates,
                            this.currentWeekDate);
                    currentOldNewLoginDays = mapKeyToLoginDayMap
                            .get(keyInfoStr).intValue();

                }

            }
            if (currentUser == 1) {
                context.write(new IntWritable(this.currentWeekDate), new Text(
                        keyInfoStr + DateFormatInfo.SEPARATOR + currentUser + DateFormatInfo.SEPARATOR + currentNewUser
                                + DateFormatInfo.SEPARATOR + currentNewOldUser + DateFormatInfo.SEPARATOR
                                + currentOldOldUser + DateFormatInfo.SEPARATOR + currentOldNewUser
                                + DateFormatInfo.SEPARATOR + currentLostUser + DateFormatInfo.SEPARATOR
                                + currentNewLostUser + DateFormatInfo.SEPARATOR
                                + currentNewOldLostUser + DateFormatInfo.SEPARATOR
                                + currentOldOldLostUser + DateFormatInfo.SEPARATOR
                                + currentOldNewLostUser + DateFormatInfo.SEPARATOR
                                + currentLoginDays + DateFormatInfo.SEPARATOR + currentNewLoginDays
                                + DateFormatInfo.SEPARATOR + currentNewOldLoginDays + DateFormatInfo.SEPARATOR
                                + currentOldOldLoginDays + DateFormatInfo.SEPARATOR
                                + currentOldNewLoginDays));
            }
        }

        private int getLostCount(Integer[] loginDates, Integer currentDateId) {
            for (int i = 0; i < loginDates.length; i++) {
                if (!loginDates[i].equals(currentDateId)) {
                    return 0;
                }
            }
            return 1;
        }

    }

    /**
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        WeekInfoArgs weekInfoArgs = new WeekInfoArgs();
        weekInfoArgs.init("weekUserInfoDetail.jar");
        weekInfoArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new WeekMobileUserLostInfoMR(), weekInfoArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_weekmobileuserlostinfomr");
        job.setJarByClass(WeekMobileUserLostInfoMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("weeksday", args[2]);
        job.setMapperClass(WeekMobileUserLostInfoMapper.class);
        job.setReducerClass(WeekMobileUserLostInfoReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(24);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
